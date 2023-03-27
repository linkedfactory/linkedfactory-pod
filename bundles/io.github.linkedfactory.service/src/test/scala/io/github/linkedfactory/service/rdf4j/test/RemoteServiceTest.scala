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
package io.github.linkedfactory.service.rdf4j.test

import io.github.linkedfactory.kvin.http.KvinHttp
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb
import io.github.linkedfactory.kvin.util.JsonFormatParser
import io.github.linkedfactory.kvin.{Kvin, KvinTuple}
import io.github.linkedfactory.service.rdf4j.KvinFederatedService
import net.enilink.commons.iterator.IExtendedIterator
import net.enilink.komma.core.{URI, URIs}
import org.eclipse.rdf4j.model.Literal
import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver
import org.eclipse.rdf4j.repository.Repository
import org.eclipse.rdf4j.repository.sail.SailRepository
import org.eclipse.rdf4j.sail.memory.MemoryStore
import org.junit.{After, Assert, Before, Test}

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets
import scala.util.Random

class RemoteServiceTest {
  val seed = 200
  val valueProperty: URI = URIs.createURI("property:value")
  val START_TIME = 1000

  var storeDirectory: File = _
  var store: Kvin = _
  var repository: Repository = _

  object kvinHttpInstance {
    var count = 0
    var fetchCall = 0
  }

  object mockData {
    var item1: String =
      """
        |{
        |    "http://example.org/item1": {
        |        "http://example.org/p1": [
        |            {
        |                "value": 57.934878949512196,
        |                "time": 1619424246120
        |            }
        |        ]
        |        }
        |}
        |""".stripMargin

    var item2: String =
      """
        |{
        |    "http://example.org/item2": {
        |        "http://example.org/p2": [
        |            {
        |                "value": 57.934878949512196,
        |                "time": 1619424246120
        |            }
        |        ]
        |    }
        |}
        |""".stripMargin
  }


  @Before
  def init(): Unit = {
    createStore()
    createRepository()
  }

  def createStore(): Unit = {
    storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis + "-" + Random.nextInt(1000) + "/")
    storeDirectory.deleteOnExit()
    store = new KvinLevelDb(storeDirectory)
  }

  def createRepository(): Unit = {
    val memoryStore = new MemoryStore
    val sailRepository = new SailRepository(memoryStore)

    sailRepository.setFederatedServiceResolver(new AbstractFederatedServiceResolver() {
      override def createService(url: String): KvinFederatedService = {
        if (getKvinServiceUrl(url).isDefined) {
          val endpoint = getKvinServiceUrl(url).get
          val service = new KvinFederatedService(new KvinHttp(endpoint) {
            override def fetch(item: URI, property: URI, context: URI, end: Long, begin: Long, limit: Long, interval: Long, op: String): IExtendedIterator[KvinTuple] = {
              kvinHttpInstance.fetchCall = kvinHttpInstance.fetchCall + 1
              var response = ""
              if (item.toString.endsWith("item1")) {
                response = mockData.item1
              } else if (item.toString.endsWith("item2")) {
                response = mockData.item2
              }
              val jsonParser = new JsonFormatParser(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)))
              jsonParser.parse
            }
          }, true)
          kvinHttpInstance.count = kvinHttpInstance.count + 1
          return service
        }
        val service = new KvinFederatedService(store, false)
        service
      }
    })

    sailRepository.init()
    this.repository = sailRepository
  }

  def getKvinServiceUrl(serviceUrl: String): Option[String] = {
    var url: Option[String] = Option(null)
    if (serviceUrl.startsWith("kvin:")) {
      url = Some(serviceUrl.replace("kvin:", ""))
    }
    url
  }


  @Test
  def basicTest(): Unit = {
    val conn = repository.getConnection

    try {
      val time = START_TIME + 20

      val values = "values ?item { <item1> }"
      val queryStr = s"select * where { $values service <kvin:http://test0.com> { ?item <p1> [ <kvin:to> $time ; <kvin:limit> 1 ; <kvin:value> ?value ; <kvin:time> ?time ] } }"
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/")
      val r = query.evaluate

      while (r.hasNext) {
        val bs = r.next
        Assert.assertTrue(bs.getValue("item").toString.equals("http://example.org/item1"))
        Assert.assertTrue(bs.getValue("time").stringValue().equals("1619424246120"))
        Assert.assertTrue(bs.getValue("value").stringValue().equals("57.934878949512196"))
      }
      r.close()

      Assert.assertTrue(kvinHttpInstance.count == 1)
      Assert.assertTrue(kvinHttpInstance.fetchCall == 1)
      r.close()
    }

    finally {
      conn.close()
    }
  }

  @Test
  def multipleEndpointTest(): Unit = {
    val conn = repository.getConnection

    try {
      val time = START_TIME + 20

      val queryStr =
        s"""
           |SELECT * {
           |  values ?item { <item1> }
           |  SERVICE <kvin:http://test1.com> {
           |    ?item <p1> [ <kvin:to> $time ; <kvin:limit> 1 ;
           |      <kvin:value> ?value ; <kvin:time> ?time
           |    ]
           |  }
           |  SERVICE <kvin:http://test2.com> {
           |    ?item <p2> [ <kvin:to> $time ; <kvin:limit> 1 ;
           |      <kvin:value> ?value1; <kvin:time> ?time1
           |    ]
           |  }
           |}
           |""".stripMargin

      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/")
      val r = query.evaluate

      while (r.hasNext) {
        val bs = r.next
        Assert.assertTrue(bs.getValue("property").toString.equals("http://example.org/p1"))
        Assert.assertTrue(bs.getValue("time").stringValue().equals("1619424246120"))
        Assert.assertTrue(bs.getValue("value").stringValue().equals("57.934878949512196"))
      }
      Assert.assertTrue(kvinHttpInstance.count == 2)
      Assert.assertTrue(kvinHttpInstance.fetchCall == 2)
      r.close()
    } finally {
      conn.close()
    }
  }


  @Test
  def aggregationTest(): Unit = {
    val conn = repository.getConnection

    try {
      val time = START_TIME + 20

      val queryStr =
        s"""
           |SELECT (SUM(?value) AS ?total) {
           |  values ?item { <item1> <item2> }
           |  optional {
           |    # has only values for item1
           |    SERVICE <kvin:http://test1.com> {
           |      ?item <p1> [ <kvin:to> $time ; <kvin:limit> 1 ; <kvin:value> ?value ]
           |    }
           |  }
           |  optional {
           |    # has only values for item2
           |    SERVICE <kvin:http://test2.com> {
           |      ?item <p2> [ <kvin:to> $time ; <kvin:limit> 1 ; <kvin:value> ?value ]
           |    }
           |  }
           |}
           |""".stripMargin

      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/")
      val r = query.evaluate

      while (r.hasNext) {
        val bs = r.next
        Assert.assertEquals(115.869757899024392, bs.getValue("total").asInstanceOf[Literal].doubleValue(), 10e-6)
      }
      Assert.assertTrue(kvinHttpInstance.count == 2)
      Assert.assertTrue(kvinHttpInstance.fetchCall == 4)
      r.close()
    } finally {
      conn.close()
    }
  }

  @After
  def closeRepository(): Unit = {
    repository.shutDown()
  }

}