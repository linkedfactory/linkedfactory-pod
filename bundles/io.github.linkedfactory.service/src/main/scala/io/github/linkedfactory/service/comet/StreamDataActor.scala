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
package io.github.linkedfactory.service.comet

import io.github.linkedfactory.kvin.{Kvin, KvinListener, KvinTuple}
import io.github.linkedfactory.service.Data
import net.enilink.komma.core.{URI, URIs}
import net.liftweb.common.Full
import net.liftweb.http.CometActor
import net.liftweb.http.js.JE.JsRaw
import net.liftweb.http.js.JsCmds.{Noop, OnLoad, Script, jsExpToJsCmd}
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.{JArray, JField, JObject}
import net.liftweb.json.JsonDSL.{jobject2assoc, list2jvalue, long2jvalue, pair2Assoc}
import net.liftweb.json.compactRender
import net.liftweb.util.Helpers.intToTimeSpanBuilder
import net.liftweb.util.Schedule

import java.util.TreeSet
import java.util.concurrent.ScheduledFuture
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * A comet actor that streams updates of time series data to clients via DOM events 'stream-init' and 'stream-update'.
 */
class StreamDataActor extends CometActor with KvinListener {
  /**
   * Stores the point of time the property was read the last time.
   */
  class PropertyInfo {
    var lastTimestamp = 0L
  }

  implicit val formats = net.liftweb.json.DefaultFormats

  override def dontCacheRendering = true

  val DEFAULT_LIMIT = 100

  var limit = 0

  @volatile var future: ScheduledFuture[_] = _
  var changedItems = mutable.Set.empty[(URI, URI)]

  var itemsOrPatterns = Set.empty[URI]
  var prefixes: TreeSet[String] = null
  var items = mutable.Map.empty[URI, mutable.Map[URI, PropertyInfo]]

  override def lifespan = Full(5.second)

  def trimStar(uri: URI) = uri.trimSegments(1).appendSegment("")

  /**
   * Expand prefixes into real data points
   */
  def computeItems(kvin: Kvin, urisOrPatterns: Iterable[URI]): List[URI] = urisOrPatterns.flatMap {
    case uri if uri.lastSegment == "**" => {
      val prefix = trimStar(uri)
      kvin.descendants(prefix).toList.asScala
    }
    case uri => Some(uri)
  }.toList

  /**
   * Retrieve item properties
   */
  def computeProperties(kvin: Kvin, item: URI): List[URI] = kvin.properties(item).iterator.asScala.toList

  def propertyInfos(item: URI) = items.getOrElseUpdate(item, mutable.Map.empty[URI, PropertyInfo])

  def propertyInfo(property: URI, propertyInfos: mutable.Map[URI, PropertyInfo]) = propertyInfos.getOrElseUpdate(property, new PropertyInfo)

  override def render = {
    val jsCmd = Data.kvin map { kvin =>
      val data = for ((item, propInfos) <- items) yield {
        val itemData = computeProperties(kvin, item) map { property =>
          val propInfo = propertyInfo(property, propInfos)
          var timestamp = propInfo.lastTimestamp
          val end = if (timestamp == 0L) KvinTuple.TIME_MAX_VALUE else timestamp

          val field = JField(property.toString, JArray(
            kvin.fetch(item, property, Kvin.DEFAULT_CONTEXT, end, 0L, limit, 0L, null)
              .iterator.asScala.map { e =>
              timestamp = timestamp.max(e.time)
              ("time", e.time) ~ ("seq", decompose(e.seqNr)) ~ ("value", decompose(e.value))
            }.toList))

          // update time stamp for last query
          propInfo.lastTimestamp = timestamp + 1
          field
        }
        JField(item.toString, JObject(itemData.toList))
      }
      OnLoad(triggerCmd("stream-init", compactRender(JObject(data.toList))))
    }
    Script(jsCmd getOrElse Noop)
  }

  override def localSetup {
    limit = attributes.get("limit").map(_.toInt).getOrElse(DEFAULT_LIMIT)
    itemsOrPatterns = attributes.get("items").map(_.split("\\s+").filter(_.nonEmpty).map(URIs.createURI(_, true)))
      .filter(_.nonEmpty).map(_.toSet).getOrElse {
      throw new IllegalArgumentException("Missing 'items' attribute.")
    }
    // collect URIs in the form of http://example.org/items/**
    itemsOrPatterns.foreach { uri =>
      if (uri.lastSegment == "**") {
        if (prefixes == null) prefixes = new TreeSet
        prefixes.add(trimStar(uri).toString)
      }
    }
    Data.kvin map { kvin =>
      items = mutable.Map(computeItems(kvin, itemsOrPatterns).map { (_, mutable.Map.empty[URI, PropertyInfo]) }: _*)
      kvin.addListener(this)
    }
  }

  override def localShutdown {
    Data.kvin.map(_.removeListener(this))
  }

  override def entityCreated(item: URI, property: URI) {
  }

  override def valueAdded(item: URI, property: URI, ctx : URI, time: Long, seqNr: Long, value: Any) {
    var trackedItem = items.contains(item)
    if (!trackedItem && prefixes != null && {
      val prefix = prefixes.floor(item.toString)
      prefix != null && item.toString.startsWith(prefix)
    }) {
      trackedItem = true
    }
    if (trackedItem) {
      changedItems.add(item, property)
      if (future == null) synchronized {
        if (future == null) future = Schedule.schedule(this, "update", 1.second)
      }
    }
  }

  override def lowPriority: PartialFunction[Any, Unit] = {
    case "update" => {
      synchronized {
        future = null
      }
      // create local copy to avoid long synchronized block
      val changedItemsLocal = changedItems.synchronized {
        val copy = changedItems.toList
        changedItems.clear
        copy
      }
      // query only the changed items and properties
      val data = changedItemsLocal.groupBy(_._1) flatMap {
        case (item, properties) =>
          val propInfos = propertyInfos(item)
          val itemData = properties.flatMap {
            case (_, property) =>
              val propInfo = propertyInfo(property, propInfos)
              var timestamp = propInfo.lastTimestamp

              val propData = Data.kvin.map(_.fetch(item, property, Kvin.DEFAULT_CONTEXT, KvinTuple.TIME_MAX_VALUE, timestamp, 100, 0L, null)
                .iterator.asScala.map { e =>
                timestamp = timestamp.max(e.time)
                ("time", e.time) ~ ("seqNr", decompose(e.seqNr)) ~ ("value", decompose(e.value))
              }.toList)
              propData map { propData =>
                propInfo.lastTimestamp = timestamp + 1
                List(JField(property.toString, JArray(propData)))
              } getOrElse(Nil)
          }
          if (itemData.isEmpty) Nil else List(JField(item.toString, itemData.toList))
      }
      partialUpdate(triggerCmd("stream-update", compactRender(JObject(data.toList))))
    }
  }

  def triggerCmd(event: String, data: String): net.liftweb.http.js.JsCmd = {
    JsRaw("""$(document).trigger('""" + event + """', [""" + data + """])""")
  }
}
