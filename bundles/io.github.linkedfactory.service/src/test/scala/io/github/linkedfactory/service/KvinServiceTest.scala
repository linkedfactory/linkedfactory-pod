package io.github.linkedfactory.service

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.inject.Guice
import io.github.linkedfactory.core.kvin.{Kvin, KvinListener, KvinTuple, Record}
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb
import io.github.linkedfactory.core.kvin.util.{JsonFormatParser, ParquetFormatParser, ParquetFormatWriter}
import net.enilink.commons.iterator.NiceIterator
import net.enilink.komma.core.{KommaModule, URI, URIs}
import net.enilink.komma.model.*
import net.enilink.platform.lift.util.Globals
import net.liftweb.common.{Box, Full}
import net.liftweb.http.provider.servlet.HTTPRequestServlet
import net.liftweb.http.{CurrentReq, InMemoryResponse, LiftResponse, OutputStreamResponse, Req, S}
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.{LocalInputFile, LocalOutputFile}
import org.apache.parquet.schema.MessageTypeParser
import org.junit.Assert.*
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
  private def createService(store: Kvin, inMemoryParquetSize: Int = 32 * 1024 * 1024, context: URI = null): KvinService =
    new KvinService("linkedfactory" :: Nil, store) {
      override protected val maxInMemoryParquetSize: Int = inMemoryParquetSize
      override protected def contextModelUri: URI = if (context == null) super.contextModelUri else context

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

  private val kvinService: KvinService = createService(KvinServiceTest.store)

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

  private def postParquet(service: KvinService, payload: Array[Byte]): LiftResponse = {
    val request = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body = payload
      contentType = "application/vnd.apache.parquet"
    }
    service(toReq(request))().openOr(null)
  }

  private def writeParquet(rows: Seq[KvinTuple]): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val writer = new ParquetFormatWriter(output)
    try rows.foreach(writer.writeTuple) finally writer.close()
    output.toByteArray
  }

  private def responseBytes(response: LiftResponse): Array[Byte] = {
    assertEquals(200, response.toResponse.code)
    assertEquals(ParquetFormatWriter.MIME_TYPE, responseContentType(response))
    val output = new ByteArrayOutputStream()
    response.asInstanceOf[OutputStreamResponse].out(output)
    output.toByteArray
  }

  private def readParquet(bytes: Array[Byte]): List[KvinTuple] = {
    val tuples = new ParquetFormatParser(URIs.createURI(baseUrl), bytes).parse()
    try tuples.toList.asScala.toList finally tuples.close()
  }

  private def assertTuples(expected: Seq[KvinTuple], actual: Seq[KvinTuple]): Unit = {
    def sorted(ts: Seq[KvinTuple]) = ts.sortBy(t => (t.item.toString, t.property.toString, t.time, t.seqNr))
    assertEquals(expected.size, actual.size)
    sorted(expected).zip(sorted(actual)).foreach { case (a, b) =>
      assertEquals(a.item, b.item)
      assertEquals(a.property, b.property)
      assertEquals(a.context, b.context)
      assertEquals(a.time, b.time)
      assertEquals(a.seqNr, b.seqNr)
      assertEquals(a.value.getClass, b.value.getClass)
      assertTrue(s"Expected ${a.value}, got ${b.value}", java.util.Objects.deepEquals(a.value, b.value))
    }
  }

  /** Writes Parquet files with arbitrary schemas, e.g. like pandas or pyarrow. */
  private def writeGroups(schema: String, rowGroupSize: Long = 128L * 1024 * 1024)
                         (rows: SimpleGroupFactory => Seq[Group]): Array[Byte] = {
    val messageType = MessageTypeParser.parseMessageType(schema)
    val file = Files.createTempFile("kvin-parquet-test-", ".parquet")
    try {
      val writer = ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(messageType)
        .withConf(new PlainParquetConfiguration()).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withRowGroupSize(rowGroupSize).build()
      try rows(new SimpleGroupFactory(messageType)).foreach(writer.write)
      finally writer.close()
      Files.readAllBytes(file)
    } finally Files.delete(file)
  }

  private def rowGroupCount(payload: Array[Byte]): Int = {
    val file = Files.createTempFile("kvin-parquet-test-", ".parquet")
    try {
      Files.write(file, payload)
      val reader = ParquetFileReader.open(new LocalInputFile(file),
        ParquetReadOptions.builder(new PlainParquetConfiguration()).build())
      try reader.getRowGroups.size finally reader.close()
    } finally Files.delete(file)
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

  private val parquetIdentity = "optional binary item (STRING); optional binary property (STRING); optional int64 time; "

  @Test
  def parquetPostUsesDefaultContextAndSequenceAndIgnoresExtraColumns(): Unit = {
    val item = URIs.createURI("urn:parquet:defaults:item")
    val property = URIs.createURI("urn:parquet:defaults:property")
    // Reordered columns and omitted unused value columns are valid.
    val payload = writeGroups("message m { optional double valueDouble; " + parquetIdentity +
      "optional binary unrelated; }") { factory =>
      Seq(factory.newGroup().append("item", item.toString).append("property", property.toString)
        .append("time", 1710000004000L).append("valueDouble", 7.25d).append("unrelated", "ignored"))
    }
    val response = postParquet(kvinService, payload).toResponse
    assertEquals(responseBody(response), 200, response.code)
    val stored = KvinServiceTest.store.fetch(item, property, Kvin.DEFAULT_CONTEXT, 0)
    try assertTuples(List(new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 1710000004000L, 0, 7.25d)),
      stored.toList.asScala.toList)
    finally stored.close()
  }

  @Test
  def parquetPostResolvesRelativeUrisAgainstRequestPath(): Unit = {
    val payload = writeGroups("message m { " + parquetIdentity + "optional double valueDouble; }") { factory =>
      Seq(factory.newGroup().append("item", "parquet-relative/machine1").append("property", "temperature")
        .append("time", 1710000005000L).append("valueDouble", 21.5d))
    }
    val response = postParquet(kvinService, payload).toResponse
    assertEquals(responseBody(response), 200, response.code)
    val item = URIs.createURI("http://foo.com/linkedfactory/parquet-relative/machine1")
    val property = URIs.createURI("http://foo.com/linkedfactory/temperature")
    val stored = KvinServiceTest.store.fetch(item, property, Kvin.DEFAULT_CONTEXT, 0)
    try assertTuples(List(new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 1710000005000L, 0, 21.5d)),
      stored.toList.asScala.toList)
    finally stored.close()
  }

  @Test
  def parquetPostReadsMultipleRowGroups(): Unit = {
    val item = URIs.createURI("urn:parquet:large:item")
    val property = URIs.createURI("urn:parquet:large:property")
    val tupleCount = 2000
    val payload = writeGroups("message m { " + parquetIdentity + "required double valueDouble; }",
      rowGroupSize = 4096) { factory =>
      (0 until tupleCount).map(i => factory.newGroup().append("item", item.toString)
        .append("property", property.toString).append("time", 1710000000000L + i).append("valueDouble", i.toDouble))
    }
    assertTrue("Expected multiple row groups", rowGroupCount(payload) > 1)
    for (service <- Seq(kvinService, createService(KvinServiceTest.store, inMemoryParquetSize = 1024))) {
      val response = postParquet(service, payload).toResponse
      assertEquals(responseBody(response), 200, response.code)
      val stored = KvinServiceTest.store.fetch(item, property, Kvin.DEFAULT_CONTEXT, 0)
      try assertTuples((0 until tupleCount).map(i =>
        new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 1710000000000L + i, 0, Double.box(i))),
        stored.toList.asScala.toList)
      finally stored.close()
    }
  }

  @Test
  def parquetPostConvertsTimestampUnitsAndLongSequenceNumbers(): Unit = {
    val property = URIs.createURI("urn:parquet:timestamp:property")
    val time = 1710000000123L
    for ((unit, unitsPerMilli) <- Seq("MILLIS" -> 1L, "MICROS" -> 1000L, "NANOS" -> 1000000L)) {
      val item = URIs.createURI(s"urn:parquet:timestamp:$unit")
      val payload = writeGroups("message m { optional binary item (STRING); optional binary property (STRING); " +
        s"optional int64 time (TIMESTAMP($unit,true)); optional int64 seqNr; optional int64 valueLong; }") { factory =>
        Seq(factory.newGroup().append("item", item.toString).append("property", property.toString)
          .append("time", time * unitsPerMilli + unitsPerMilli - 1).append("seqNr", 3L).append("valueLong", 42L))
      }
      val response = postParquet(kvinService, payload).toResponse
      assertEquals(responseBody(response), 200, response.code)
      val stored = KvinServiceTest.store.fetch(item, property, Kvin.DEFAULT_CONTEXT, 0)
      try assertTuples(List(new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, time, 3, Long.box(42L))),
        stored.toList.asScala.toList)
      finally stored.close()
    }
  }

  @Test
  def parquetPostRejectsMissingOrMultipleTypedValues(): Unit = {
    for (valueCount <- Seq(0, 2)) {
      val payload = writeGroups("message m { " + parquetIdentity +
        "optional int32 valueInt; optional boolean valueBool; }") { factory =>
        val row = factory.newGroup().append("item", "urn:parquet:invalid:item")
          .append("property", "urn:parquet:invalid:property").append("time", 1L)
        if (valueCount == 2) row.append("valueInt", 1).append("valueBool", true)
        Seq(row)
      }
      assertEquals(400, postParquet(kvinService, payload).toResponse.code)
    }
  }

  @Test
  def parquetPostRejectsInvalidObjectLengths(): Unit = {
    // A declared length of 1 GiB with no contents must fail without allocating it.
    for (marker <- Seq('O', '[', 'R', '"')) {
      val encoded = java.nio.ByteBuffer.allocate(16)
      encoded.put(marker.toByte)
      io.github.linkedfactory.core.kvin.util.Varint.writeUnsigned(encoded, 1L << 30)
      val bytes = java.util.Arrays.copyOf(encoded.array(), encoded.position())
      val payload = writeGroups("message m { " + parquetIdentity + "optional binary valueObject; }") { factory =>
        Seq(factory.newGroup().append("item", "urn:invalid:length").append("property", "urn:invalid:property")
          .append("time", 1L).append("valueObject", org.apache.parquet.io.api.Binary.fromConstantByteArray(bytes)))
      }
      assertEquals(400, postParquet(kvinService, payload).toResponse.code)
    }
  }

  @Test
  def parquetPostHidesInternalErrorDetails(): Unit = {
    for (service <- Seq(kvinService, createService(KvinServiceTest.store, inMemoryParquetSize = 0))) {
      val response = postParquet(service, Array[Byte](1, 2, 3)).toResponse
      assertEquals(400, response.code)
      val body = responseBody(response)
      assertTrue(body, body.contains("Invalid Parquet payload"))
      assertFalse(body, body.contains("kvin-upload"))
    }
  }

  @Test
  def parquetValuesDownloadUploadRoundTripPreservesMixedTuples(): Unit = {
    val context = URIs.createURI("urn:parquet:roundtrip:context")
    val item = URIs.createURI("urn:parquet:roundtrip:item")
    val property = URIs.createURI("urn:parquet:roundtrip:property")
    val values: Seq[Object] = Seq(Int.box(42), Long.box(9007199254740993L), Float.box(4.5f),
      Double.box(5.5d), Boolean.box(false), "ready ✓", URIs.createURI("urn:value:uri"), Short.box(7.toShort),
      new Record(URIs.createURI("urn:" + "p" * 140), "nested"),
      Array[Object](Int.box(1), "two", URIs.createURI("urn:three")))
    // Types change within the same series, including observations sharing a timestamp.
    val expected = values.zipWithIndex.map { case (value, i) =>
      new KvinTuple(item, property, context, 1710000000000L + i / 2, i % 2, value)
    } :+ new KvinTuple(URIs.createURI("urn:parquet:roundtrip:item2"),
      URIs.createURI("urn:parquet:roundtrip:property2"), context, 1710000000000L, 0, true)
    val sourceDirectory = Files.createTempDirectory("kvin-parquet-source-")
    val destinationDirectory = Files.createTempDirectory("kvin-parquet-destination-")
    val source = new KvinLevelDb(sourceDirectory.toFile)
    val destination = new KvinLevelDb(destinationDirectory.toFile)
    try {
      val sourceService = createService(source, context = context)
      // Upload the same file containing all datatypes first, then download via the actual endpoint.
      assertEquals(200, postParquet(sourceService, writeParquet(expected)).toResponse.code)
      val request = new MockHttpServletRequest(baseUrl) {
        method = "GET"
        headers = Map("Accept" -> List(ParquetFormatWriter.MIME_TYPE))
        parameters = List("items" -> expected.map(_.item.toString).distinct.mkString(" "))
      }
      val payload = responseBytes(sourceService(toReq(request))().openOr(null))
      assertTuples(expected, readParquet(payload))
      // Force the tempfile path for re-upload.
      assertEquals(200, postParquet(createService(destination, inMemoryParquetSize = 0), payload).toResponse.code)
      val actual = expected.map(t => (t.item, t.property)).distinct.flatMap { case (i, p) =>
        val iterator = destination.fetch(i, p, context, 0)
        try iterator.toList.asScala finally iterator.close()
      }
      assertTuples(expected, actual)
    } finally {
      source.close()
      destination.close()
      KvinServiceTest.deleteDirectory(sourceDirectory)
      KvinServiceTest.deleteDirectory(destinationDirectory)
    }
  }

  @Test
  def parquetDownloadNegotiationEmptyResultsAndFilters(): Unit = {
    val item = URIs.createURI("urn:parquet:filter:item")
    val property = URIs.createURI("urn:parquet:filter:property")
    val tuples = (1 to 3).map(i => new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, i.toLong, 0, Int.box(i)))
    KvinServiceTest.store.put(tuples.asJava)
    for (mode <- Seq("accept", "type")) {
      val request = new MockHttpServletRequest(baseUrl) {
        method = "GET"
        headers = if (mode == "accept") Map("Accept" -> List(ParquetFormatWriter.MIME_TYPE)) else Map.empty
        parameters = List("item" -> item.toString, "property" -> property.toString, "from" -> "2", "to" -> "3", "limit" -> "1") :::
          (if (mode == "type") List("type" -> ParquetFormatWriter.MIME_TYPE) else Nil)
      }
      assertTuples(Seq(tuples.last), readParquet(responseBytes(kvinService(toReq(request))().openOr(null))))
    }
    val empty = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List("item" -> "urn:parquet:no-data", "type" -> ParquetFormatWriter.MIME_TYPE)
    }
    val bytes = responseBytes(kvinService(toReq(empty))().openOr(null))
    assertTrue(readParquet(bytes).isEmpty)
    assertEquals(200, postParquet(kvinService, bytes).toResponse.code)
  }

  @Test
  def parquetPostRejectsMalformedInputAndSchema(): Unit = {
    assertEquals(400, postParquet(kvinService, Array[Byte](1, 2, 3)).toResponse.code)
    val invalidSchemas = Seq(
      parquetIdentity + "optional double value;", // old single-value format
      parquetIdentity, // no value column
      parquetIdentity + "optional int64 valueDouble;", // wrong physical type
      parquetIdentity + "optional int32 valueInt (DECIMAL(9,2));",
      parquetIdentity + "optional int64 valueLong (INTEGER(64,false));",
      parquetIdentity + "optional int64 valueLong (TIMESTAMP(MILLIS,true));",
      parquetIdentity + "repeated double valueDouble;",
      parquetIdentity + "optional group valueObject { optional binary data; }",
      "optional binary item; optional int64 time; optional double valueDouble;"
    )
    invalidSchemas.foreach { schema =>
      val payload = writeGroups("message m { " + schema + " }")(_ => Seq.empty)
      assertEquals(schema, 400, postParquet(kvinService, payload).toResponse.code)
    }
    for (bad <- Seq("item", "property", "time", "uri", "context", "seqNr", "valueObject")) {
      val payload = writeGroups("message m { " + parquetIdentity +
        "optional binary context; optional int64 seqNr; optional int32 valueInt; optional binary valueObject; }") { factory =>
        val row = factory.newGroup()
        if (bad != "item") row.append("item", if (bad == "uri") "bad uri" else "urn:invalid:item")
        if (bad != "property") row.append("property", "urn:invalid:property")
        if (bad != "time") row.append("time", 1L)
        if (bad == "context") row.append("context", "relative")
        if (bad == "seqNr") row.append("seqNr", Long.MaxValue)
        if (bad == "valueObject") row.append("valueObject", "invalid encoding") else row.append("valueInt", 1)
        Seq(row)
      }
      assertEquals(bad, 400, postParquet(kvinService, payload).toResponse.code)
    }
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
  def postRequestInvalidModel(): Unit = {
    // reject post request with invalid model
    val invalidPostReq = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      body_=(TestData.item1, "application/json")
      // use queryString because parameters are not applied for POST requests
      override def queryString: String = "model=invalid:model"
    }
    val response = kvinRest(toReq(invalidPostReq))().map(_.toResponse).openOr(null)
    assertEquals(400, response.code)

    val body = responseBody(response)
    assertTrue(body.contains("\"code\":\"INVALID_MODEL\""))
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
