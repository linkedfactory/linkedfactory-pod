/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.service.mqtt

import com.google.common.cache.{Cache, CacheBuilder}
import io.github.linkedfactory.service.ItemDataEvents
import io.github.linkedfactory.service.util.ItemDataParser
import net.enilink.komma.core.{IReference, URIs}
import net.enilink.komma.em.concepts.IResource
import net.enilink.platform.core.PluginConfigModel
import net.liftweb.common.Full
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.{JField, JObject}
import net.liftweb.json.JsonDSL.pair2Assoc
import net.liftweb.json.{DefaultFormats, JsonParser, compactRender}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.osgi.framework.{FrameworkUtil, ServiceRegistration}
import org.osgi.service.component.annotations.{Component, Reference}
import org.osgi.service.event.{Event, EventAdmin, EventConstants, EventHandler}

import java.util.concurrent.TimeUnit
import java.util.{HashMap, Hashtable, UUID}
import scala.util.matching.Regex

/**
 * A simple bridge between HTTP and MQTT interfaces for linked factory events.
 */
@Component
class MqttEventBridge {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val ownAuthorities: Cache[String, Boolean] = CacheBuilder.newBuilder.expireAfterWrite(30, TimeUnit.SECONDS).build.asInstanceOf[Cache[String, Boolean]]

  val ITEM: Regex = "^LF/[^/]+/([^/]+)/(.*)".r

  var client: MqttClient = _
  var eventHandlerSvc: ServiceRegistration[_] = _

  var config: PluginConfigModel = null

  var eventAdmin: EventAdmin = null

  var shuttingDown = false

  def activate() {
    val (broker, filter) = {
      config.begin()
      try {
        val m = config.getManager
        val ns = m.getNamespace("").trimSegments(1).appendSegments(Array("mqtt", ""))
        val bridgeCfg = m.find(ns.appendLocalPart("MqttEventBridge"), classOf[IResource])
        val broker = bridgeCfg.getSingle(ns.appendLocalPart("broker")) match {
          case r: IReference if r.getURI != null => Some(r.toString)
          case _ => None
        }
        val filter = bridgeCfg.getSingle(ns.appendLocalPart("filter")) match {
          case s: String => Some(s.r)
          case _ => None
        }
        (broker, filter)
      } finally {
        config.end()
      }
    }
    // will do nothing if broker is not defined
    broker.foreach { broker =>
      val persistence = new MemoryPersistence()
      // TODO - Should the client ID be configurable?
      val clientId = UUID.randomUUID.toString

      client = new MqttClient(broker, clientId, persistence)
      client.setCallback(new MqttCallback() {
        def messageArrived(topic: String, msg: MqttMessage) {
          topic match {
            // ignores own messages
            case ITEM(authority, path) if ownAuthorities.getIfPresent(authority) != true =>
              JsonParser.parseOpt(new String(msg.getPayload)) map {
                json =>
                  val topic = "linkedfactory/itemEvent/external"
                  val item = "http://" + authority + "/" + path

                  ItemDataParser.parseItem(URIs.createURI(item), json) match {
                    case Full(values) => values.map {
                      case (item, property, time, value) =>
                        val properties = new HashMap[String, Any]
                        properties.put(ItemDataEvents.ITEM, item.toString)
                        properties.put(ItemDataEvents.PROPERTY, property.toString)
                        properties.put(ItemDataEvents.TIME, time)
                        properties.put(ItemDataEvents.VALUE, value)

                        eventAdmin.postEvent(new Event(topic, properties))
                    }
                    case _ => // handle failure
                  }
              }
            case _ => // ignore those events
          }
        }

        def deliveryComplete(deliveryToken: IMqttDeliveryToken) {
        }

        def connectionLost(error: Throwable) {
        }
      })

      // connect the client after construction
      connect

      if (client.isConnected) {
        // register event handler
        val params = new Hashtable[String, Any]
        params.put(EventConstants.EVENT_TOPIC, "linkedfactory/itemEvent/internal")
        val handler = new EventHandler {
          override def handleEvent(event: Event) {
            val item = event.getProperty(ItemDataEvents.ITEM).toString
            val property = event.getProperty(ItemDataEvents.PROPERTY).toString
            val time = event.getProperty(ItemDataEvents.TIME).asInstanceOf[Long]
            val value = event.getProperty(ItemDataEvents.VALUE)

            // check item against filter, if set
            if (!filter.isDefined || filter.get.findFirstIn(item).isDefined) {
              val itemUri = URIs.createURI(item)
              val authority = itemUri.authority
              ownAuthorities.put(authority, true)
              // E is for Event - What other classifiers are possible?
              val topic = "LF" + "/E/" + authority + itemUri.segments.mkString("/", "/", "")
              publish(topic, property, time, value)
            }
          }
        }
        eventHandlerSvc = FrameworkUtil.getBundle(getClass).getBundleContext.registerService(classOf[EventHandler], handler, params)
      }
    }
  }

  def connect() {
    client.synchronized {
      if (!client.isConnected) {
        val options = new MqttConnectOptions
        options.setCleanSession(true)
        client.connect(options)
        client.subscribe("LF/#")
      }
    }
  }

  /**
   * Publishes item events over MQTT
   */
  def publish(topic: String, property: String, time: Long, value: Any) {
    val qos = 2;

    // FIXME: avoid deadlock on shutdown
    if (!shuttingDown) {
      // re-connect the client if the connection was closed
      connect

      val json = JObject(JField(property.toString, ("time", decompose(time)) ~ ("value", decompose(value))) :: Nil)
      val content = compactRender(json)
      val message = new MqttMessage(content.getBytes)
      message.setQos(qos)
      client.publish(topic, message)
    }
  }

  def deactivate() {
    shuttingDown = true
    if (eventHandlerSvc != null) eventHandlerSvc.unregister()
    eventHandlerSvc = null
    if (client != null) client.disconnect()
    client = null
  }

  @Reference
  def setConfig(config: PluginConfigModel) {
    this.config = config
  }

  @Reference
  def setEventAdmin(eventAdmin: EventAdmin) {
    this.eventAdmin = eventAdmin
  }
}
