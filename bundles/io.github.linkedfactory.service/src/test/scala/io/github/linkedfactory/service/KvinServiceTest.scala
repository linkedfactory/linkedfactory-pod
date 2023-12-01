package io.github.linkedfactory.service

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.inject.Guice
import io.github.linkedfactory.core.kvin.{Kvin, KvinTuple}
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb
import io.github.linkedfactory.core.kvin.util.JsonFormatParser
import net.enilink.commons.iterator.NiceIterator
import net.enilink.komma.core.{KommaModule, URI}
import net.enilink.komma.model._
import net.enilink.platform.lift.util.Globals
import net.liftweb.common.{Box, Full}
import net.liftweb.http.provider.servlet.HTTPRequestServlet
import net.liftweb.http.{CurrentReq, InMemoryResponse, LiftResponse, OutputStreamResponse, Req}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, IOException}
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
  val base: KvinServiceTestBase = new KvinServiceTestBase()

  @BeforeClass
  def setup(): Unit = {
    // create configuration and a model set factory
    val module: KommaModule = ModelPlugin.createModelSetModule(classOf[ModelPlugin].getClassLoader)
    val factory: IModelSetFactory = Guice.createInjector(new ModelSetModule(module)).getInstance(classOf[IModelSetFactory])

    // create a model set with an in-memory repository
    modelSet = factory.createModelSet(MODELS.NAMESPACE_URI.appendFragment("MemoryModelSet"))
    Globals.contextModelSet.default.set(Full(modelSet))

    // adding test data
    TestData.item1 = KvinServiceTest.base.generateJsonFromSingleTuple()
    TestData.itemSet = KvinServiceTest.base.generateJsonFromTupleSet()

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
  var item1: String = ""
  var itemSet: String = ""
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

  @Test
  def queryDataBasicTest(): Unit = {

    val postReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=(TestData.item1, "application/json")
    }
    assertEquals(Full(200), kvinRest(toReq(postReq))().map(_.toResponse.code))

    val getReq = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List(("item", "http://example.org/item1"), ("property", "http://example.org/properties/p1"))
      headers = (("Accept", "application/json" :: Nil) :: Nil).toMap
    }

    //var response = kvinRest(toReq(getReq))().toList.map((response) => response.toString)
    val response = kvinRest(toReq(getReq))().map(_.toResponse).openOr(null)
    val stringResponse: String = response match {
      case r: OutputStreamResponse =>
        val rStream = new ByteArrayOutputStream()
        r.out(rStream)
        rStream.toString()
      case r: InMemoryResponse =>
        r.data.toString
      case _ =>
        throw new RuntimeException("Invalid response type")
    }

    val kvinTuples: NiceIterator[KvinTuple] = new JsonFormatParser(new ByteArrayInputStream(stringResponse.getBytes())).parse()
    while (kvinTuples.hasNext) {
      val tuple: KvinTuple = kvinTuples.next()
      assertEquals(tuple.item.toString, "http://example.org/item1")
      assertEquals(tuple.property.toString, "http://example.org/properties/p1")
      assertEquals(tuple.time, 1619424246120l)
      assertEquals(tuple.value.toString, "57.934878949512196")
    }
  }

  @Test
  def queryDataWithLimitTest(): Unit = {

    val postReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=(TestData.itemSet, "application/json")
    }
    assertEquals(Full(200), kvinRest(toReq(postReq))().map(_.toResponse.code))

    val getReq = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List(("item", "http://example.org/item2"), ("property", "http://example.org/properties/p2"), ("limit", "2"))
      headers = (("Accept", "application/json" :: Nil) :: Nil).toMap
    }

    val response = kvinRest(toReq(getReq))().map(_.toResponse).openOr(null)
    val stringResponse: String = response match {
      case r: OutputStreamResponse =>
        val rStream = new ByteArrayOutputStream()
        r.out(rStream)
        rStream.toString()
      case r: InMemoryResponse =>
        r.data.toString
      case _ =>
        throw new RuntimeException("Invalid response type")
    }

    val kvinTuples: NiceIterator[KvinTuple] = new JsonFormatParser(new ByteArrayInputStream(stringResponse.getBytes())).parse()
    assertEquals(kvinTuples.toList.size(), 2)
  }

  @Test
  def queryDataWithOpTest(): Unit = {

    val postReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=(TestData.itemSet, "application/json")
    }
    assertEquals(Full(200), kvinRest(toReq(postReq))().map(_.toResponse.code))

    val getReq = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List(("item", "http://example.org/item2"), ("property", "http://example.org/properties/p2"), ("op", "sum"), ("interval", "10000"))
      headers = (("Accept", "application/json" :: Nil) :: Nil).toMap
    }

    //var response = kvinRest(toReq(getReq))().toList.map((response) => response.toString)
    val response = kvinRest(toReq(getReq))().map(_.toResponse).openOr(null)
    val stringResponse: String = response match {
      case r: OutputStreamResponse =>
        val rStream = new ByteArrayOutputStream()
        r.out(rStream)
        rStream.toString()
      case r: InMemoryResponse =>
        r.data.toString
      case _ =>
        throw new RuntimeException("Invalid response type")
    }

    val kvinTuples: NiceIterator[KvinTuple] = new JsonFormatParser(new ByteArrayInputStream(stringResponse.getBytes())).parse()
    var count = 0
    while (kvinTuples.hasNext) {
      val tuple: KvinTuple = kvinTuples.next()
      if (count == 0) {
        assertEquals(18.91, tuple.value.asInstanceOf[Number].doubleValue(), 1e-2)
      } else {
        assertEquals(18.53, tuple.value.asInstanceOf[Number].doubleValue(), 1e-2)
      }
      count = count + 1
    }
    assertEquals(2, count)

  }

  @Test
  def getPropertiesTest(): Unit = {

    val postReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=(TestData.item1, "application/json")
    }
    assertEquals(Full(200), kvinRest(toReq(postReq))().map(_.toResponse.code))

    val getReq = new MockHttpServletRequest("http://foo.com/linkedfactory/properties") {
      method = "GET"
      parameters = List(("item", "http://example.org/item2"))
      headers = (("Accept", "application/json" :: Nil) :: Nil).toMap
    }

    val response = kvinRest(toReq(getReq))().map(_.toResponse).openOr(null)
    val stringResponse: String = response match {
      case r: OutputStreamResponse =>
        val rStream = new ByteArrayOutputStream()
        r.out(rStream)
        rStream.toString()
      case r: InMemoryResponse =>
        new String(r.data)
      case _ =>
        throw new RuntimeException("Invalid response type")
    }

    val mapper: ObjectMapper = new ObjectMapper()
    val node: JsonNode = mapper.readTree(stringResponse)
    assertEquals(node.size(), 1)

  }
}