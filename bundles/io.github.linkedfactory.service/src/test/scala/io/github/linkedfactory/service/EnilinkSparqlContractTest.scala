package io.github.linkedfactory.service

import com.google.inject.Guice
import net.enilink.komma.core.{KommaModule, Statement, URIs}
import net.enilink.komma.model._
import net.enilink.platform.lift.util.Globals
import net.enilink.platform.web.rest.SparqlRest
import net.liftweb.common.{Box, Full}
import net.liftweb.http.provider.servlet.HTTPRequestServlet
import net.liftweb.http.{BasicResponse, CurrentReq, InMemoryResponse, LiftResponse, OutputStreamResponse, Req}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import javax.servlet.http.HttpServletRequest

/**
 * Thin compatibility tests that verify the SPARQL error contract as consumed
 * by linkedfactory-pod from the enilink dependency.
 */
object EnilinkSparqlContractTest {
  var modelSet: IModelSet = _
  val sensorModel = MODELS.NAMESPACE_URI.appendFragment("sensor-data")

  @BeforeClass
  def setup(): Unit = {
    val module: KommaModule = ModelPlugin.createModelSetModule(classOf[ModelPlugin].getClassLoader)
    val factory: IModelSetFactory = Guice.createInjector(new ModelSetModule(module))
      .getInstance(classOf[IModelSetFactory])

    modelSet = factory.createModelSet(MODELS.NAMESPACE_URI.appendFragment("MemoryModelSet"))
    Globals.contextModelSet.default.set(Full(modelSet))

    val model = modelSet.createModel(sensorModel)
    val em = model.getManager
    em.add(new Statement(
      URIs.createURI("http://example.org/sensors/temp-01"),
      URIs.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
      URIs.createURI("http://example.org/vocab/Sensor")
    ))
  }

  @AfterClass
  def tearDown(): Unit = {
    if (modelSet != null) {
      modelSet.dispose()
      modelSet = null
    }
  }
}

class EnilinkSparqlContractTest {
  private val sparqlService = new SparqlRest() {
    override def apply(in: Req): () => Box[LiftResponse] = {
      try {
        Globals.contextModelSet.vend.map(_.getUnitOfWork.begin)
        CurrentReq.doWith(in) {
          super.apply(in)
        }
      } finally {
        Globals.contextModelSet.vend.map(_.getUnitOfWork.end)
      }
    }
  }

  private val baseUrl = "http://foo.com/sparql"

  private def toReq(httpRequest: HttpServletRequest): Req = {
    Req(new HTTPRequestServlet(httpRequest, null), Nil, System.nanoTime)
  }

  private def execute(req: HttpServletRequest): BasicResponse = {
    sparqlService(toReq(req))().map(_.toResponse)
      .openOr(throw new AssertionError("Expected SPARQL response but got empty result"))
  }

  @Test
  def missingModelReturns400(): Unit = {
    val req = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List("query" -> "SELECT ?s WHERE { ?s ?p ?o }")
      headers = Map("Accept" -> List("application/sparql-results+json"))
    }
    assertErrorContract(execute(req), 400, "MISSING_MODEL:")
  }

  @Test
  def missingQueryReturns400(): Unit = {
    val req = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List("model" -> EnilinkSparqlContractTest.sensorModel.toString)
      headers = Map("Accept" -> List("application/sparql-results+json"))
    }
    assertErrorContract(execute(req), 400, "MISSING_QUERY:")
  }

  @Test
  def unknownModelReturns404(): Unit = {
    val req = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List(
        "query" -> "SELECT ?s WHERE { ?s ?p ?o }",
        "model" -> "http://example.org/models/does-not-exist"
      )
      headers = Map("Accept" -> List("application/sparql-results+json"))
    }
    assertErrorContract(execute(req), 404, "MODEL_NOT_FOUND:")
  }

  @Test
  def unsupportedAcceptReturns406(): Unit = {
    val req = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List(
        "query" -> "SELECT ?s WHERE { ?s ?p ?o }",
        "model" -> EnilinkSparqlContractTest.sensorModel.toString
      )
      headers = Map("Accept" -> List("application/vnd.custom+xml"))
    }
    assertErrorContract(execute(req), 406, "UNSUPPORTED_ACCEPT:")
  }

  @Test
  def malformedSparqlReturns400(): Unit = {
    val req = new MockHttpServletRequest(baseUrl) {
      method = "GET"
      parameters = List(
        "query" -> "SELECTT ?s WHERE { ?s ?p ?o }",
        "model" -> EnilinkSparqlContractTest.sensorModel.toString
      )
      headers = Map("Accept" -> List("application/sparql-results+json"))
    }
    assertErrorContract(execute(req), 400, "MALFORMED_QUERY:")
  }

  @Test
  def invalidQueryRequestReturns400(): Unit = {
    val req = new MockHttpServletRequest(baseUrl) {
      method = "POST"
      parameters = List(
        "query" -> "SELECT ?s WHERE { ?s ?p ?o }",
        "update" -> "INSERT DATA { <a:s> <a:p> <a:o> }",
        "model" -> EnilinkSparqlContractTest.sensorModel.toString
      )
      contentType = "application/x-www-form-urlencoded"
      headers = Map("Accept" -> List("application/sparql-results+json"))
    }
    assertErrorContract(execute(req), 400, "INVALID_QUERY_REQUEST:")
  }

  private def assertErrorContract(response: BasicResponse, expectedStatus: Int, expectedPrefix: String): Unit = {
    assertNotNull("Response was null", response)
    assertEquals("HTTP status", expectedStatus, response.code)

    val contentType = response.headers
      .find(_._1.equalsIgnoreCase("Content-Type"))
      .map(_._2)
      .getOrElse("")
    assertTrue(s"Expected text/plain content type but got: '$contentType'", contentType.toLowerCase.startsWith("text/plain"))

    val body = responseBody(response)
    assertTrue(s"Expected prefix '$expectedPrefix' but got: $body", body.startsWith(expectedPrefix))
  }

  private def responseBody(response: BasicResponse): String = response match {
    case r: InMemoryResponse => new String(r.data, StandardCharsets.UTF_8)
    case r: OutputStreamResponse =>
      val out = new ByteArrayOutputStream()
      r.out(out)
      out.toString(StandardCharsets.UTF_8.name)
    case _ =>
      fail("Unexpected response type")
      ""
  }
}