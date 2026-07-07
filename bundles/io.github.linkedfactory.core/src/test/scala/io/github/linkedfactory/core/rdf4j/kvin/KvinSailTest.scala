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
package io.github.linkedfactory.core.rdf4j.kvin

import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb
import io.github.linkedfactory.core.kvin.{Kvin, KvinTuple}
import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver
import net.enilink.komma.core.{URI, URIs}
import org.eclipse.rdf4j.repository.Repository
import org.eclipse.rdf4j.repository.sail.SailRepository
import org.eclipse.rdf4j.sail.memory.MemoryStore
import org.junit.{After, Before, Test}

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import scala.util.Random

import scala.compiletime.uninitialized

class KvinSailTest {
  val seed = 200
  val valueProperty: URI = URIs.createURI("property:value")
  val START_TIME = 1000

  var storeDirectory: File = uninitialized
  var store: Kvin = uninitialized
  var repository: Repository = uninitialized

  @Test
  def basicTest(): Unit = {
    val conn = repository.getConnection
    val vf = repository.getValueFactory

    try {
      conn.add(vf.createIRI(itemUri(1).toString), vf.createIRI(valueProperty.toString),
        vf.createLiteral(5), vf.createIRI("kvin:"))
    } finally {
      conn.close()
    }
  }

  def itemUri(nr: Int): URI = URIs.createURI("http://example.org/item-" + nr)

  def addData(items: Int, values: Int): List[KvinTuple] = {
    val rand = new Random(seed)
    (1 until items).flatMap{ nr =>
      var time = START_TIME
      val uri = itemUri(nr)
      for (i <- 1 to values) yield {
        val value = rand.nextDouble() * rand.nextInt(100)
        val seqNr = i % 1000
        val tuple = new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, seqNr, value)
        store.put(tuple)
        time += 10
        tuple
      }
    }.toList
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
    val kvinSail = new KvinSail(store, memoryStore)
    val sailRepository = new SailRepository(kvinSail)

    sailRepository.setFederatedServiceResolver(new BaseFederatedServiceResolver() {
      override def createService(url: String): KvinFederatedService = {
        val service = new KvinFederatedService(store, () => getExecutorService, null,false)
        service
      }
    })

    sailRepository.init()
    this.repository = sailRepository
  }

  @After
  def closeRepository(): Unit = {
    repository.shutDown()
  }

  @After
  def closeStore(): Unit = {
    store.close()
    store = null
    deleteDirectory(storeDirectory.toPath)
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