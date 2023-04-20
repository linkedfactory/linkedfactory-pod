package io.github.linkedfactory.service

import io.github.linkedfactory.kvin.Kvin
import io.github.linkedfactory.kvin.influxdb.KvinInfluxDb
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb
import net.enilink.komma.core.{IReference, URIs}
import net.enilink.komma.em.concepts.IResource
import net.enilink.platform.lift.util.Globals
import org.eclipse.core.runtime.Platform
import org.osgi.framework.FrameworkUtil

import java.io.File
import java.nio.file.Files
import scala.util.{Failure, Success, Try}

object KvinManager {
  val bundleContext = Option(FrameworkUtil.getBundle(getClass)).map(_.getBundleContext).getOrElse(null)
  val instanceLoc = if (bundleContext != null) Platform.getInstanceLocation else null
  val cfgURI = URIs.createURI("plugin://de.fraunhofer.iwu.linkedfactory.service/data/")
  private var kvin: Option[Kvin] = None

  def getKvin(): Option[Kvin] = kvin

  {
    Globals.withPluginConfig { pcModel => {
      val cfg = pcModel.getManager.find(cfgURI.appendLocalPart("store")).asInstanceOf[IResource]
      kvin = cfg.getSingle(cfgURI.appendLocalPart("type")) match {
        case s: String if (s == "InfluxDB") =>
          val endpoint = cfg.getSingle(cfgURI.appendLocalPart("endpoint")) match {
            case r: IReference if r.getURI != null => r.getURI
            case s: String => URIs.createURI(s)
            case _ => URIs.createURI("http://localhost:8086/")
          }
          val db = cfg.getSingle(cfgURI.appendLocalPart("database")) match {
            case s: String => s
            case _ => "LinkedFactory"
          }
          Try(new KvinInfluxDb(endpoint, db)) match {
            case Failure(throwable) =>
              System.err.println(s"Value store: FAILURE for InfluxDB @ $endpoint: ${throwable.getMessage}")
              None
            case Success(success) =>
              println(s"Value store: using InfluxDB @ $endpoint")
              Some(success)
          }
        //case s: String if (s == "KVIN") =>
        case _ =>
          val dirName = cfg.getSingle(cfgURI.appendLocalPart("dirName")) match {
            case s: String => s
            case _ => "linkedfactory-valuestore"
          }
          val valueStorePath = new File(dirName) match {
            case dir if dir.isAbsolute() => dir
            case _ if instanceLoc.isSet => new File(new File(instanceLoc.getURL.toURI), dirName)
            case _ => new File(Files.createTempDirectory("lf").toFile, dirName)
          }
          Try {
            new KvinLevelDb(valueStorePath)
          } match {
            case Failure(throwable) =>
              System.err.println(s"Value store: FAILURE for LF w/ path=$valueStorePath: ${throwable.getMessage}")
              None
            case Success(success) =>
              println(s"Value store: using LF w/ path=$valueStorePath")
              Some(success)
          }
      }
    }
    }
  }
}
