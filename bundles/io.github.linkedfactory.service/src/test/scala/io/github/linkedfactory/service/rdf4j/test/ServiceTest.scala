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

import io.github.linkedfactory.kvin.leveldb.KvinLevelDb
import io.github.linkedfactory.kvin.{Kvin, KvinTuple, Record}
import io.github.linkedfactory.service.rdf4j.KvinFederatedService
import net.enilink.komma.core.URIs
import net.enilink.vocab.rdf.RDF
import org.eclipse.rdf4j.model.{IRI, Literal}
import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver
import org.eclipse.rdf4j.repository.Repository
import org.eclipse.rdf4j.repository.sail.SailRepository
import org.eclipse.rdf4j.sail.memory.MemoryStore
import org.junit.{After, Assert, Before, Test}

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import scala.util.Random

class ServiceTest {
  val seed = 200
  val valueProperty = URIs.createURI("property:value")
  val START_TIME = 1000

  var storeDirectory: File = _
  var store: Kvin = _
  var repository: Repository = _

  @Test
  def basicTest {
    val data = addData(10, 10)

    val conn = repository.getConnection
    val vf = repository.getValueFactory
    try {
      val time = START_TIME + 20

      val values = "values ?item { <item-1> <item-2> }"
      val queryStr =
        s"""select * where { $values service <kvin:> {
           |?item <property:value> ?v . ?v <kvin:to> $time ; <kvin:limit> 1 .
           |?v <kvin:value> ?value ; <kvin:time> ?time } }""".stripMargin
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/")

      val dataByItemAndTime = data.filter(_.time <= time).groupBy(_.item)
        .view.mapValues(_.groupBy(_.time))

      val r = query.evaluate
      while (r.hasNext) {
        val bs = r.next
        val item = URIs.createURI(bs.getValue("item").toString)
        val time = bs.getValue("time").asInstanceOf[Literal].longValue()

        val itemValue = dataByItemAndTime(item)(time).head.value

        Assert.assertEquals(itemValue.asInstanceOf[Double],
          bs.getValue("value").asInstanceOf[Literal].doubleValue, 0.001)
      }
      r.close
    } finally {
      conn.close
    }
  }

  @Test
  def recordTest {
    val data = addRecords(2, 10)

    val conn = repository.getConnection
    val vf = repository.getValueFactory
    try {
      val time = START_TIME + 20

      val values = "values ?item { <item-1> <item-2> }"
      val queryStr =
        s"""select * where { $values service <kvin:> {
           |?item <property:value> ?v . ?v <kvin:to> $time ; <kvin:limit> 1 .
           |?v <kvin:value> ?record ; <kvin:time> ?time .
           |?record <p:3> [ <p:nested> ?value ]
           |} }""".stripMargin
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/")

      val dataByItemAndTime = data.filter(_.time <= time).groupBy(_.item)
        .view.mapValues(_.groupBy(_.time))

      val r = query.evaluate
      while (r.hasNext) {
        val bs = r.next
        val item = URIs.createURI(bs.getValue("item").toString)
        val time = bs.getValue("time").asInstanceOf[Literal].longValue()

        val itemValue = dataByItemAndTime(item)(time).head.value

        Assert.assertEquals(itemValue.asInstanceOf[Record]
          .first(URIs.createURI("p:3")).getValue.asInstanceOf[Record]
          .first(URIs.createURI("p:nested")).getValue.asInstanceOf[Double],
          bs.getValue("value").asInstanceOf[Literal].doubleValue, 0.001)
      }
      r.close
    } finally {
      conn.close
    }
  }

  @Test
  def arrayTest {
    val data = addArrays(2, 10)

    val conn = repository.getConnection
    val vf = repository.getValueFactory
    try {
      val time = START_TIME + 20

      val values = "values ?item { <item-1> <item-2> }"
      val queryStr =
        s"""prefix rdf: <${RDF.NAMESPACE}>
           |select * where { $values service <kvin:> {
           |?item <property:value> ?v . ?v <kvin:to> $time ; <kvin:limit> 1 .
           |?v <kvin:value> ?array ; <kvin:time> ?time .
           |?array rdf:_3 [ <p:nested> ?v3 ] ; rdf:_1 ?v1
           |} }""".stripMargin
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/")

      val dataByItemAndTime = data.filter(_.time <= time).groupBy(_.item)
        .view.mapValues(_.groupBy(_.time))

      val r = query.evaluate
      Assert.assertTrue(r.hasNext)
      while (r.hasNext) {
        val bs = r.next
        val item = URIs.createURI(bs.getValue("item").toString)
        val time = bs.getValue("time").asInstanceOf[Literal].longValue()

        val itemValue = dataByItemAndTime(item)(time).head.value

        val array = itemValue.asInstanceOf[Array[_]]
        Assert.assertEquals(array(0).toString, bs.getValue("v1").asInstanceOf[Literal].getLabel)
        Assert.assertEquals(array(2).asInstanceOf[Record]
          .first(URIs.createURI("p:nested")).getValue.asInstanceOf[Double],
          bs.getValue("v3").asInstanceOf[Literal].doubleValue, 0.001)
      }
      r.close
    } finally {
      conn.close
    }
  }

  @Test
  def testLimit {
    val data = addData(10, 10)

    val conn = repository.getConnection
    val vf = repository.getValueFactory
    try {
      val maxLimit = 5
      // ensure that at least maxLimit values are returned
      val time = START_TIME + (maxLimit + 1) * 10

      val baseUri = "http://example.org/"
      val itemCount = 2
      val values = s"values ?item { ${
        1.to(itemCount).map(i => "<item-" + i + ">").mkString(" ")
      } }"
      val queryStr =
        s"""select distinct * where { $values service <kvin:> {
           |?item <property:value> ?v1 .
           |?v1 <kvin:value> ?value ; <kvin:time> ?time ;
           |<kvin:to> $time ; <kvin:limit> ?limit }
           |}""".stripMargin
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri)

      for (limit <- 1 to maxLimit) {
        query.setBinding("limit", vf.createLiteral(limit))
        val r = query.evaluate
        try {
          1.to(itemCount * limit).foreach { _ =>
            Assert.assertTrue(r.hasNext)
            r.next
          }
          Assert.assertFalse(r.hasNext)
        } finally {
          r.close
        }
      }
    } finally {
      conn.close
    }
  }

  @Test
  def testJoin {
    val data = addData(10, 10)

    val conn = repository.getConnection
    try {
      val time = START_TIME + 20

      val baseUri = "http://example.org/"
      val queryStr = s"""select * where { 
  service <kvin:> { 
    <item-1> <property:value> [ <kvin:time> ?time ; <kvin:to> $time ; <kvin:value> ?v1_value ] .
    <item-2> <property:value> [ <kvin:time> ?time ; <kvin:value> ?v2_value ] .
  }
}"""
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri)

      val dataByItemAndTime = data.groupBy(_.item).view.mapValues(_.groupBy(_.time))

      val r = query.evaluate
      while (r.hasNext) {
        val bs = r.next
        val time = bs.getValue("time").asInstanceOf[Literal].longValue()

        val item1Value = dataByItemAndTime(itemUri(1))(time).head.value
        val item2Value = dataByItemAndTime(itemUri(2))(time).head.value

        Assert.assertEquals(item1Value.asInstanceOf[Double],
          bs.getValue("v1_value").asInstanceOf[Literal].doubleValue, 0.001)
        Assert.assertEquals(item2Value.asInstanceOf[Double],
          bs.getValue("v2_value").asInstanceOf[Literal].doubleValue, 0.001)
      }
    } finally {
      conn.close
    }
  }

  @Test
  def testJoinPreviousValue {
    val data = addData(10, 10)

    val conn = repository.getConnection
    val vf = repository.getValueFactory
    try {
      val time = START_TIME + 50

      val baseUri = "http://example.org/"
      val queryStr = s"""select distinct * where {
  service <kvin:> {
    <item-1> <property:value> [ <kvin:value> ?v1_value ; <kvin:time> ?time ; <kvin:to> $time ] .
    <item-2> <property:value> [ <kvin:value> ?v2_value ; <kvin:limit> 1 ; <kvin:to> ?time ] .
  }
}"""
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri)

      val dataByItemAndTime = data.groupBy(_.item).view.mapValues(_.groupBy(_.time))

      var expectedItem1Values = dataByItemAndTime(itemUri(1)).filter(_._1 <= time)

      val r = query.evaluate
      while (r.hasNext) {
        val bs = r.next
        val time = bs.getValue("time").asInstanceOf[Literal].longValue()

        val item1Value = expectedItem1Values(time).head.value
        expectedItem1Values = expectedItem1Values.removed(time)
        val item2Value = dataByItemAndTime(itemUri(2))(time).head.value

        Assert.assertEquals(item1Value.asInstanceOf[Double],
          bs.getValue("v1_value").asInstanceOf[Literal].doubleValue, 0.001)
        Assert.assertEquals(item2Value.asInstanceOf[Double],
          bs.getValue("v2_value").asInstanceOf[Literal].doubleValue, 0.001)
      }

      Assert.assertTrue("Expected values should all be returned by query",
        expectedItem1Values.isEmpty)
    } finally {
      conn.close
    }
  }

  def itemUri(nr: Int) = URIs.createURI("http://example.org/item-" + nr)

  def addData(items: Int, values: Int): List[KvinTuple] = {
    val rand = new Random(seed)
    (1 to items).flatMap{ nr =>
      var time = START_TIME
      val uri = itemUri(nr)
      for (i <- 1 to values) yield {
        val value = rand.nextDouble * rand.nextInt(100)
        val seqNr = i % 1000
        val tuple = new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, seqNr, value)
        store.put(tuple)
        time += 10
        tuple
      }
    }.toList
  }

  def addRecords(items: Int, values: Int): List[KvinTuple] = {
    val rand = new Random(seed)
    (1 until items).flatMap{ nr =>
      var time = START_TIME
      val uri = itemUri(nr)
      for (i <- 1 to values) yield {
        val value = new Record(URIs.createURI("p:" + i),
          new Record(URIs.createURI("p:nested"), rand.nextDouble * rand.nextInt(100)))
        val seqNr = i % 1000
        val tuple = new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, seqNr, value)
        store.put(tuple)
        time += 10
        tuple
      }
    }.toList
  }

  def addArrays(items: Int, values: Int): List[KvinTuple] = {
    val rand = new Random(seed)
    (1 until items).flatMap{ nr =>
      var time = START_TIME
      val uri = itemUri(nr)
      for (i <- 1 to values) yield {
        val value = Array(1, "2",
          new Record(URIs.createURI("p:nested"), rand.nextDouble * rand.nextInt(100)))
        val seqNr = i % 1000
        val tuple = new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, seqNr, value)
        store.put(tuple)
        time += 10
        tuple
      }
    }.toList
  }

  @Before
  def init {
    createStore
    createRepository
  }

  def createStore {
    storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis + "-" + Random.nextInt(1000) + "/")
    storeDirectory.deleteOnExit
    store = new KvinLevelDb(storeDirectory)
  }

  def createRepository {
    val memoryStore = new MemoryStore
    val sailRepository = new SailRepository(memoryStore)

    sailRepository.setFederatedServiceResolver(new AbstractFederatedServiceResolver() {
      override def createService(url: String) = {
        val service = new KvinFederatedService(store, false)
        service
      }
    })

    sailRepository.init
    this.repository = sailRepository
  }

  @After
  def closeRepository {
    repository.shutDown
  }

  @After
  def closeStore {
    store.close
    store = null
    deleteDirectory(storeDirectory.toPath)
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