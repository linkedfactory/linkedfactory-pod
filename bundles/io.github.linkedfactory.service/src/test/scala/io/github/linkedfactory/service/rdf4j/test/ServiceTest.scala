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
import io.github.linkedfactory.kvin.{Kvin, KvinTuple}
import io.github.linkedfactory.service.rdf4j.KvinFederatedService
import net.enilink.komma.core.URIs
import org.eclipse.rdf4j.model.Literal
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
    addData(10, 10)

    val conn = repository.getConnection
    val vf = repository.getValueFactory
    try {
      val time = START_TIME + 20

      val values = "values ?item { <item-1> <item-2> }"
      val queryStr = s"select * where { $values service <kvin:> { ?item <property:value> ?v . ?v <kvin:to> $time ; <kvin:limit> 1 . ?v <kvin:value> ?value ; <kvin:time> ?time } }"
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/")

      val start = System.currentTimeMillis
      val count = 1000
      for (i <- 0 to count) {
        val r = query.evaluate
        while (r.hasNext) {
          val bs = r.next
          //if ((i % 100) == 0) println(bs.toString)
        }
        r.close
      }
      val duration = (System.currentTimeMillis - start) / 1000.0
      println("Queries per second: " + (count / duration))
    } finally {
      conn.close
    }
  }

  @Test
  def testLimit {
    addData(10, 10)

    val conn = repository.getConnection
    val vf = repository.getValueFactory
    try {
      val time = START_TIME + 20

      val baseUri = "http://example.org/"
      val values = "values ?item { <item-1> <item-2> }"
      val queryStr = s"select * where { $values service <kvin:> { ?item <property:value> ?v1 . ?v1 <kvin:value> ?value ; <kvin:time> ?time ; <kvin:to> $time ; <kvin:limit> 1 } }"
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri)

      val r = query.evaluate
      try {
        for (i <- 1 to 2) {
          Assert.assertTrue(r.hasNext)
          r.next
        }
        Assert.assertFalse(r.hasNext)
      } finally {
        r.close
      }
    } finally {
      conn.close
    }
  }

  @Test
  def testJoin {
    val data = addData(10, 10)

    val conn = repository.getConnection
    val vf = repository.getValueFactory
    try {
      val time = START_TIME + 20

      val baseUri = "http://example.org/"
      val queryStr = s"""select * where { 
  service <kvin:> { 
    <item-1> <property:value> [ <kvin:value> ?v1_value ; <kvin:time> ?time ; <kvin:to> $time ] .
    <item-2> <property:value> [ <kvin:time> ?time ; <kvin:value> ?v2_value ] .
  }
}"""
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri)

      val dataByItemAndTime = data.groupBy(_.item).mapValues(_.groupBy(_.time))

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
      val time = START_TIME + 20

      val baseUri = "http://example.org/"
      val queryStr = s"""select * where {
  service <kvin:> {
    <item-1> <property:value> [ <kvin:value> ?v1_value ; <kvin:time> ?time ; <kvin:to> $time ] .
    <item-2> <property:value> [ <kvin:to> ?time ; <kvin:limit> 1 ; <kvin:value> ?v2_value ] .
  }
}"""
      val query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri)

      val dataByItemAndTime = data.groupBy(_.item).mapValues(_.groupBy(_.time))

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

  def itemUri(nr: Int) = URIs.createURI("http://example.org/item-" + nr)

  def addData(items: Int, values: Int): List[KvinTuple] = {
    val rand = new Random(seed)
    (1 until items).flatMap{ nr =>
      var time = START_TIME
      val uri = itemUri(nr)
      for (i <- 1 to values) yield {
        val value = rand.nextDouble * rand.nextInt(100)
        val tuple = new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, value)
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
        val service = new KvinFederatedService(store)
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