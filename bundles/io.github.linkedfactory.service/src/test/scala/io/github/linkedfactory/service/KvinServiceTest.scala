package io.github.linkedfactory.service

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.inject.Guice
import io.github.linkedfactory.core.kvin.{Kvin, KvinListener, KvinTuple}
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb
import io.github.linkedfactory.core.kvin.util.JsonFormatParser
import net.enilink.commons.iterator.NiceIterator
import net.enilink.komma.core.{KommaModule, URI, URIs}
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
import jakarta.servlet.http.HttpServletRequest
import scala.util.Random
import scala.compiletime.uninitialized
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

/**
 * Companion object of unit tests for the KVIN service endpoint
 */
object KvinServiceTest {
  var modelSet: IModelSet = null
  var storeDirectory: File = uninitialized
  var store: Kvin = uninitialized
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

    createStore()
  }

  @AfterClass
  def tearDown(): Unit = {
    modelSet.dispose()
    modelSet = null

    store.close()
    store = null
    deleteDirectory(storeDirectory.toPath)
  }

  def createStore(): Unit = {
    storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis + "-" + Random.nextInt(1000) + "/")
    storeDirectory.deleteOnExit()
    store = new KvinLevelDb(storeDirectory)
  }

  def deleteDirectory(dir: Path): Unit = {
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
  private val kvinService: KvinService = new KvinService("linkedfactory" :: Nil, KvinServiceTest.store) {
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

  private def responseBody(response: LiftResponse): String = response match {
    case r: OutputStreamResponse =>
      val rStream = new ByteArrayOutputStream()
      r.out(rStream)
      rStream.toString()
    case r: InMemoryResponse =>
      new String(r.data)
    case _ =>
      throw new RuntimeException("Invalid response type")
  }

  private def responseContentType(response: LiftResponse): String = response.toResponse match {
    case r: OutputStreamResponse => r.headers.find(_._1.equalsIgnoreCase("Content-Type")).map(_._2).getOrElse("")
    case r: InMemoryResponse => r.headers.find(_._1.equalsIgnoreCase("Content-Type")).map(_._2).getOrElse("")
    case _ => ""
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
    val postResponse = kvinRest(toReq(postReq))().map(_.toResponse).openOr(null)
    assertEquals(200, postResponse.code)
    val postBody = responseBody(postResponse)
    assertTrue(postBody.trim.isEmpty)

    // support get request
    val getReq = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      headers = (("Accept", "application/json" :: Nil) :: Nil).toMap
    }
    assertEquals(Full(200), kvinRest(toReq(getReq))().map(_.toResponse.code))
  }

  @Test
  def csvPostPersistsSparseRowsInBoundedSeriesOrder(): Unit = {
    val itemA = URIs.createURI("urn:csv:test:item:a")
    val itemB = URIs.createURI("urn:csv:test:item:b")
    val property = URIs.createURI("urn:csv:test:property")
    val observed = ArrayBuffer.empty[(URI, URI, Long, Long, Object)]
    val listener = new KvinListener {
      override def entityCreated(item: URI): Unit = ()

      override def valueAdded(item: URI, property: URI, context: URI, time: Long,
                              seqNr: Long, value: Object): Unit = {
        if (item == itemA || item == itemB) observed += ((item, context, time, seqNr, value))
      }
    }
    val csv = String.join("\n",
      "time,seqNr,<urn:csv:test:item:a>@<urn:csv:test:property>,<urn:csv:test:item:b>@<urn:csv:test:property>",
      "100,1,10",
      "101,2,,20")
    val request = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=(csv, "text/csv")
    }

    KvinServiceTest.store.addListener(listener)
    try assertEquals(200, kvinRest(toReq(request))().map(_.toResponse).openOr(null).code)
    finally KvinServiceTest.store.removeListener(listener)

    assertEquals(List(
      (itemA, 100L, 1L, 10L),
      (itemA, 101L, 2L, ""),
      (itemB, 101L, 2L, 20L)), observed.map(t => (t._1, t._3, t._4, t._5)).toList)

    val stored = KvinServiceTest.store.fetch(itemA, property, observed.head._2, 0)
    try assertEquals(List("", 10L), stored.toList.asScala.map(_.value).toList)
    finally stored.close()
  }

  @Test
  def csvPostRejectsMalformedTimestamp(): Unit = {
    val request = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=("time,value\ninvalid,1", "text/csv")
    }
    assertEquals(400, kvinRest(toReq(request))().map(_.toResponse).openOr(null).code)
  }

  @Test
  def explicitEnvelopeResponsesUseJsonContentType(): Unit = {
    val invalidPostReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=("""{ "item" : [false, 1]} }""", "application/json")
    }
    val invalidResponse = kvinRest(toReq(invalidPostReq))().map(_.toResponse).openOr(null)
    assertEquals(400, invalidResponse.code)
    assertTrue(responseContentType(invalidResponse).toLowerCase.contains("application/json"))
  }

  @Test
  def deleteValuesReturnsDeletedCountWithoutEnvelope(): Unit = {
    val postReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=(TestData.itemSet, "application/json")
    }
    assertEquals(200, kvinRest(toReq(postReq))().map(_.toResponse).openOr(null).code)

    val deleteReq = new MockHttpServletRequest(baseUrl) {
      method = "DELETE"
      parameters = List(("item", "http://example.org/item2"), ("property", "http://example.org/properties/p2"))
      headers = (("Accept", "application/json" :: Nil) :: Nil).toMap
    }

    val response = kvinRest(toReq(deleteReq))().map(_.toResponse).openOr(null)
    assertEquals(200, response.code)

    val body = responseBody(response)
    assertTrue(body.contains("\"deleted\":"))
    assertFalse(body.contains("\"success\":"))
    assertFalse(body.contains("\"code\":"))
  }

  @Test
  def postRequestInvalidData(): Unit = {
    // reject post request with invalid data
    val invalidPostReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=("""{ "item" : [false, 1]} }""", "application/json")
    }
    val response = kvinRest(toReq(invalidPostReq))().map(_.toResponse).openOr(null)
    assertEquals(400, response.code)

    val body = responseBody(response)
    assertTrue(body.contains("\"code\":\"INVALID_PAYLOAD\""))
    assertTrue(body.contains("\"message\":"))
    assertFalse(body.contains("\"status\":"))
  }

  @Test
  def queryDataWithTooLargeLimitReturnsExplicitError(): Unit = {
    val getReq = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List(("limit", "500001"))
      headers = (("Accept", "application/json" :: Nil) :: Nil).toMap
    }

    val response = kvinRest(toReq(getReq))().map(_.toResponse).openOr(null)
    assertEquals(400, response.code)

    val body = responseBody(response)
    assertTrue(body.contains("\"code\":\"LIMIT_TOO_LARGE\""))
    assertTrue(body.contains("maximum limit"))
  }

  @Test
  def queryDataWithUnsupportedResponseTypeReturnsExplicitError(): Unit = {
    val getReq = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      headers = (("Accept", "application/xml" :: Nil) :: Nil).toMap
    }

    val response = kvinRest(toReq(getReq))().map(_.toResponse).openOr(null)
    assertEquals(400, response.code)

    val body = responseBody(response)
    assertTrue(body.contains("\"code\":\"UNSUPPORTED_RESPONSE_TYPE\""))
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
    val stringResponse: String = responseBody(response)

    val kvinTuples = new JsonFormatParser(new ByteArrayInputStream(stringResponse.getBytes())).parse()
    while (kvinTuples.hasNext) {
      val tuple: KvinTuple = kvinTuples.next()
      assertEquals(tuple.item.toString, "http://example.org/item1")
      assertEquals(tuple.property.toString, "http://example.org/properties/p1")
      assertEquals(tuple.time, 1619424246120L)
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
    val stringResponse: String = responseBody(response)

    val kvinTuples = new JsonFormatParser(new ByteArrayInputStream(stringResponse.getBytes())).parse()
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
    val stringResponse: String = responseBody(response)

    val kvinTuples = new JsonFormatParser(new ByteArrayInputStream(stringResponse.getBytes())).parse()
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
  def retrievePropertiesTest(): Unit = {
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
    val stringResponse: String = responseBody(response)

    val mapper: ObjectMapper = new ObjectMapper()
    val node: JsonNode = mapper.readTree(stringResponse)
    assertEquals(node.size(), 1)

  }
}
