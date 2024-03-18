package io.github.linkedfactory.service.benchmark

import com.google.inject.Guice
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb
import io.github.linkedfactory.core.kvin.util.JsonFormatWriter
import io.github.linkedfactory.core.kvin.{Kvin, KvinTuple}
import io.github.linkedfactory.service.{KvinService, MockHttpServletRequest}
import net.enilink.commons.iterator.WrappedIterator
import net.enilink.komma.core.{KommaModule, URI, URIs}
import net.enilink.komma.model._
import net.enilink.platform.lift.util.Globals
import net.liftweb.common.{Box, Full}
import net.liftweb.http.provider.servlet.HTTPRequestServlet
import net.liftweb.http.{CurrentReq, LiftResponse, Req}
import org.junit.{AfterClass, BeforeClass, Ignore, Test}
import sun.invoke.util.ValueConversions

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util
import java.util.concurrent.LinkedBlockingQueue
import javax.servlet.http.HttpServletRequest
import scala.util.Random

/**
 * Companion object of unit tests for the KVIN service endpoint
 */
object KvinServiceBenchmark {
  var modelSet: IModelSet = null
  var storeDirectory: File = _
  var store: Kvin = _

  @BeforeClass
  def setup(): Unit = {
    // create configuration and a model set factory
    val module: KommaModule = ModelPlugin.createModelSetModule(classOf[ModelPlugin].getClassLoader)
    val factory: IModelSetFactory = Guice.createInjector(new ModelSetModule(module)).getInstance(classOf[IModelSetFactory])

    // create a model set with an in-memory repository
    modelSet = factory.createModelSet(MODELS.NAMESPACE_URI.appendFragment("MemoryModelSet"))
    Globals.contextModelSet.default.set(Full(modelSet))

    createStore
  }

  @AfterClass
  def tearDown(): Unit = {
    modelSet.dispose()
    modelSet = null

    store.close
    store = null
    deleteDirectory(storeDirectory.toPath)
  }

  def createStore {
    storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis + "-" + Random.nextInt(1000) + "/")
    storeDirectory.deleteOnExit
    store = new KvinLevelDb(storeDirectory)
  }

  def deleteDirectory(dir: Path) {
    // delete store directory
    Files.walkFileTree(dir, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, ex: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
  }
}

/**
 * Unit tests for the KVIN service endpoint
 */
class KvinServiceBenchmark {
  val kvinService = new KvinService("linkedfactory" :: Nil, KvinServiceBenchmark.store) {
    override def apply(in: Req): () => Box[LiftResponse] = {
      try {
        Globals.contextModelSet.vend.map(_.getUnitOfWork.begin)
        // S.request is used in Data.pathToURI therefore the request needs to be initialized here
        CurrentReq.doWith(in) {
          super.apply(in)
        }
      } finally {
        Globals.contextModelSet.vend.map(_.getUnitOfWork.end)
      }
    }
  }

  def kvinRest(req: Req): () => Box[LiftResponse] = {
    kvinService(req)
  }

  val baseUrl = "http://foo.com/linkedfactory/values"

  def toReq(httpRequest: HttpServletRequest): Req = {
    Req(new HTTPRequestServlet(httpRequest, null), Nil, System.nanoTime)
  }

  @Test
  @Ignore
  def postValues(): Unit = {
    val valueProperty = URIs.createURI("property:value")

    val seed = 200
    val writeValues = 1000000

    val benchmarkStart = System.currentTimeMillis

    val startTimeValues = 1478252048736L
    val nrs = Array.fill(100)(Random.nextInt(Integer.MAX_VALUE))
    val rand = new Random(seed)

    // decouples client-side serialization and server-side parsing and insertion
    val queue = new LinkedBlockingQueue[Option[String]](2)
    val inserter = new Thread() {
      override def run() : Unit = {
        var finished = false
        do {
          queue.take() match {
            case None => finished = true
            case Some(json) =>
            // support post request
            val postReq = new MockHttpServletRequest(baseUrl) {
              method = "POST"
              body_=(json, "application/json")
            }
            kvinRest(toReq(postReq))().map(_.toResponse.code)
          }
        } while (!finished)
      }
    }
    inserter.start()

    var tuples = new util.ArrayList[KvinTuple]()
    var currentTime = startTimeValues
    (0 to writeValues).foreach { i =>
      val randomNr = nrs(rand.nextInt(nrs.length))
      val uri = URIs.createURI("http://linkedfactory.github.io/" + randomNr + "/e3fabrik/rollex/" + randomNr + "/measured-point-1")
      val ctx = URIs.createURI("ctx:" + randomNr)

      val value = if (randomNr % 2 == 0) rand.nextGaussian else rand.nextLong(100000)

      tuples.add(new KvinTuple(uri, valueProperty, ctx, currentTime, value))
      currentTime += rand.nextInt(1000)

      if (i % 10000 == 0) {
        println("  at: " + i)
        val json = JsonFormatWriter.toJsonString(WrappedIterator.create(tuples.iterator()))
        queue.put(Some(json))
        tuples = new util.ArrayList[KvinTuple]()
      }
    }
    queue.put(None)
    inserter.join()

    val seconds = (System.currentTimeMillis - benchmarkStart) / 1000.0
    println(s"Wrote $writeValues in %1$$,.2f seconds: %2$$,.2f ops per second".format(seconds, writeValues / seconds))
  }
}