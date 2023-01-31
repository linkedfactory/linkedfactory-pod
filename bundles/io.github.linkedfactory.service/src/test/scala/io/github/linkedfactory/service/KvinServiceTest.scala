package io.github.linkedfactory.service

import com.google.inject.Guice
import io.github.linkedfactory.kvin.Kvin
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb
import net.enilink.komma.core.{KommaModule, URI}
import net.enilink.komma.model._
import net.enilink.platform.lift.util.Globals
import net.liftweb.common.{Box, Full}
import net.liftweb.http.provider.servlet.HTTPRequestServlet
import net.liftweb.http.{CurrentReq, LiftResponse, Req}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import javax.servlet.http.HttpServletRequest
import scala.util.Random

/**
 * Companion object of unit tests for the KVIN service endpoint
 */
object KvinServiceTest {
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

object TestData {
  val item1: String =
    """
      |{
      |    "http://example.org/item1": {
      |        "http://example.org/properties/p1": [
      |            {
      |                "value": 57.934878949512196,
      |                "time": 1619424246120
      |            }
      |        ]
      |     }
      |}
      |""".stripMargin
}

/**
 * Unit tests for the KVIN service endpoint
 */
class KvinServiceTest {
  val kvinService = new KvinService("linkedfactory" :: Nil, KvinServiceTest.store) {
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

    // do not try to publish events via OSGi event broker
    override def publishEvent(item: URI, property: URI, time: Long, value: Any): Unit = {
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
  def optionsRequest(): Unit = {
    // support options request
    val req = new MockHttpServletRequest(baseUrl) {
      method = "OPTIONS"
    }
    assertEquals(Full(200), kvinRest(toReq(req))().map(_.toResponse.code))
  }

  @Test
  def postAndGetRequest(): Unit = {
    // support post request
    val postReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=(TestData.item1, "application/json")
    }
    assertEquals(Full(200), kvinRest(toReq(postReq))().map(_.toResponse.code))

    // support get request
    val getReq = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      headers = (("Accept", "application/json" :: Nil) :: Nil).toMap
    }
    assertEquals(Full(200), kvinRest(toReq(getReq))().map(_.toResponse.code))
  }

  @Test
  def postRequestInvalidData(): Unit = {
    // reject post request with invalid data
    val invalidPostReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=("""{ "item" : [false, 1]} }""", "application/json")
    }
    assertEquals(Full(400), kvinRest(toReq(invalidPostReq))().map(_.toResponse.code))
  }
}