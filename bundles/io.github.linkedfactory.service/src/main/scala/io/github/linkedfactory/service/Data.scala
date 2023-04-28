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
package io.github.linkedfactory.service

import com.google.common.cache.CacheBuilder
import io.github.linkedfactory.kvin.{Kvin, KvinListener}
import io.github.linkedfactory.service.model.ssn._
import io.github.linkedfactory.service.util.ResourceHelpers.withTransaction
import net.enilink.komma.core._
import net.enilink.komma.em.concepts.IResource
import net.enilink.komma.model.IModel
import net.enilink.platform.core.security.SecurityUtil
import net.enilink.platform.lift.util.Globals
import net.enilink.vocab.owl.OWL
import net.enilink.vocab.rdf.RDF
import net.liftweb.common.Box
import net.liftweb.http.{RequestVar, S}
import org.eclipse.core.runtime.Platform
import org.osgi.framework.FrameworkUtil

import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.concurrent.Executors
import javax.security.auth.Subject
import scala.jdk.CollectionConverters._
import scala.reflect.{ClassTag, classTag}

object Data {
  val NAMESPACE = "http://linkedfactory.github.io/vocab#"
  val PROPERTY_CONTAINS = URIs.createURI(NAMESPACE + "contains")

  val cfgURI = URIs.createURI("plugin://de.fraunhofer.iwu.linkedfactory.service/data/")
  val dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")

  val bundleContext = Option(FrameworkUtil.getBundle(getClass)).map(_.getBundleContext).getOrElse(null)
  val instanceLoc = if (bundleContext != null) Platform.getInstanceLocation else null

  def toPEA[T](func: () => T): PrivilegedExceptionAction[T] = { () => func() }

  // FIXME: make withPluginConfig return a result, use val from result
  private var _modelURI: URI = _

  if (bundleContext != null) {
    // get configuration settings from plugin config model
    Globals.withPluginConfig { pcModel => {
      val cfg = pcModel.getManager.find(cfgURI.appendLocalPart("store")).asInstanceOf[IResource]
      _modelURI = cfg.getSingle(cfgURI.appendLocalPart("model")) match {
        case r: IReference if r.getURI != null => r.getURI
        case s: String => URIs.createURI(s)
        case _ => URIs.createURI("http://linkedfactory.github.io/data/")
      }
    }
    }
  }

  import java.util.concurrent.TimeUnit

  private val createHierarchyExecutor = Executors.newSingleThreadExecutor()
  private val SEEN_HIERARCHY_ITEMS = CacheBuilder.newBuilder
    .maximumSize(100000)
    .expireAfterWrite(10, TimeUnit.SECONDS)
    .build[URI, Any]()

  val modelURI = _modelURI
  // caches currentModel for each request
  object modelForRequest extends RequestVar[Box[IModel]](currentModel)

  def getKvin() : Option[Kvin] = {
    Option(bundleContext).flatMap(ctx =>
      Option(ctx.getServiceReference(classOf[Kvin])).map(ref => ctx.getService(ref)))
  }

  val kvin: Option[Kvin] = getKvin() map { kvin =>
    kvin.addListener(new KvinListener {
      override def entityCreated(item: URI): Unit = {
        // FIXME: add/use actual subject via session/token/...
        modelForRequest.foreach { m =>
          createHierarchyExecutor.submit(new Runnable {
            override def run(): Unit = {
              Subject.doAs(SecurityUtil.SYSTEM_USER_SUBJECT, toPEA(() => {
                val uow = m.getModelSet.getUnitOfWork
                uow.begin()
                try {
                  withTransaction(m.getManager) { manager =>
                    createHierarchy(item, manager)
                  }
                } finally {
                  uow.end()
                }
              }))
            }
          })
        }
      }

      override def valueAdded(item: URI, property: URI, context: URI, time: Long, seqNr: Long, value: Any): Unit = {
      }
    })

    Subject.doAs(SecurityUtil.SYSTEM_USER_SUBJECT, toPEA(() =>
      Globals.contextModelSet.vend.map { modelSet =>
        try {
          // disable change support
          modelSet.getDataChangeSupport.setDefaultEnabled(false)

          modelSet.getUnitOfWork.begin

          modelSet.getModule.includeModule(new KommaModule() {
            addConcept(classOf[Observation])
            addConcept(classOf[Sensor])
            addConcept(classOf[SensorOutput])
            addConcept(classOf[ObservationValue])
            addConcept(classOf[TrackingValue])
          })

          val dataModel = modelSet.createModel(modelURI)
          dataModel.setLoaded(true)
          // dataModel.getManager.clear

          init(dataModel, kvin)
        } finally {
          modelSet.getUnitOfWork.end
        }
      }
    ))
    kvin
  }

  private def createHierarchy(uri: URI, em: IEntityManager) {
    // add hierarchy to RDF store
    var current = uri
    var done = false
    while (!done && current.segmentCount > 1) {
      val parent = current.trimSegments(1)
      if (SEEN_HIERARCHY_ITEMS.getIfPresent(current) == null &&
        !em.hasMatch(parent, PROPERTY_CONTAINS, current)) {
        em.add(new Statement(parent, PROPERTY_CONTAINS, current))
        SEEN_HIERARCHY_ITEMS.put(current, true)
      } else done = true
      current = parent
    }
  }

  private def init(model: IModel, kvin: Kvin) {
    import io.github.linkedfactory.service.util.ResourceHelpers._
    withTransaction(model.getManager) {
      manager =>
        def vocab(localPart: String) = URIs.createURI(NAMESPACE + localPart)

        manager.add(new Statement(vocab("description"), RDF.PROPERTY_TYPE, OWL.TYPE_DATATYPEPROPERTY))
        manager.add(new Statement(vocab("sensorType"), RDF.PROPERTY_TYPE, OWL.TYPE_DATATYPEPROPERTY))
        manager.add(new Statement(vocab("machineType"), RDF.PROPERTY_TYPE, OWL.TYPE_DATATYPEPROPERTY))

        manager.setNamespace("factory", URIs.createURI(NAMESPACE))

        // re-create existing hierarchy from valuestore
        kvin.descendants(URIs.createURI("")).iterator.asScala.foreach {
          uri => createHierarchy(uri, manager)
        }
    }
  }

  // FIXME: move these into helper class(es)
  def withService[S: ClassTag](f: (S) => Any) {
    val svcRef = bundleContext.getServiceReference(classTag[S].runtimeClass)
    if (svcRef != null) {
      try {
        val svc = bundleContext.getService(svcRef)
        if (svc != null) f(svc.asInstanceOf[S])
      } finally {
        bundleContext.ungetService(svcRef)
      }
    }
  }

  def currentModel: Box[IModel] = Globals.contextModelSet.vend.map(_.getModel(modelURI, false)).filter(_ != null)

  def pathToURI(relativePath: Seq[String]) = {
    S.request map { r => r.hostAndPath } map (URIs.createURI(_).appendSegments(relativePath.toArray)) openOrThrowException ("Invocation outside of request.")
  }
}
