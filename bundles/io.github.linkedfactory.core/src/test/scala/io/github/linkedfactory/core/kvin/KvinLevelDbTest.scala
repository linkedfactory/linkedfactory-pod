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
package io.github.linkedfactory.core.kvin

import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb
import net.enilink.komma.core.URIs
import org.junit.Assert._
import org.junit.{After, Before, Test}

import java.io.File
import scala.util.Random
import scala.jdk.CollectionConverters._

/**
 * Tests for the LevelDB-based time series store.
 */
class KvinLevelDbTest extends KvinTestBase {
  @Before
  def createStore {
    storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis + "-" + Random.nextInt(1000) + "/")
    storeDirectory.deleteOnExit
    store = new KvinLevelDb(storeDirectory)
  }

  def recreateStore: Unit = {
    store.close()
    store = new KvinLevelDb(storeDirectory)
  }

  @After
  def closeStore {
    store.close
    store = null
    deleteDirectory(storeDirectory.toPath)
  }

  @Test
  def testRecordsParallel {
    val ctx = null

    val points = 100
    val pointDistance: Long = 100
    val startTime: Long = pointDistance * points
    val items = 50
    // insert items in parallel
    (1 to items).asJava.stream().parallel().forEach((nr: Int) => {
      val uri = itemUri(nr)
      for (i <- 0 until points) {
        val time = startTime - i * pointDistance
        val value = new Record(URIs.createURI("prop:itemNr"), nr)
          .append(new Record(URIs.createURI("prop:pointNr"), i))
          .append(new Record(URIs.createURI("prop:value"), URIs.createURI("some:value#" + time)))
        // println("ADD: " + new KvinTuple(uri, valueProperty, ctx, time, value))
        store.put(new KvinTuple(uri, valueProperty, ctx, time, value))
      }
    })

    // close and re-open store
    recreateStore

    val queryPoints = points / 2
    val interval = (queryPoints - 1) * pointDistance
    for (nr <- 1 to items) {
      assertEquals(queryPoints, store.fetch(itemUri(nr), valueProperty, ctx,
        startTime, startTime - interval, 0, 0, null).toList.size)

      store.fetch(itemUri(nr), valueProperty, ctx, startTime, startTime - interval, 0, 0, null)
        .iterator.asScala
        .foreach { tuple =>
          val record = tuple.value.asInstanceOf[Record]
          assertEquals(URIs.createURI("some:value#" + tuple.time),
            record.first(URIs.createURI("prop:value")).getValue())
        }
    }
  }

  @Test
  def testIds: Unit = {
    val points = 100
    val pointDistance: Long = 100
    val startTime: Long = pointDistance * points
    val items = 50
    (1 to items).foreach((nr: Int) => {
      val uri = itemUri(nr)
      for (i <- 0 until points) {
        val time = startTime - i * pointDistance
        val value = new Record(URIs.createURI("prop:itemNr"), nr)
          .append(new Record(URIs.createURI("prop:pointNr"), i))
          .append(new Record(URIs.createURI("prop:value"), URIs.createURI("some:value#" + time)))
        // println("ADD: " + new KvinTuple(uri, valueProperty, ctx, time, value))
        store.put(new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, value))
      }
    })

    val nextIds = store.asInstanceOf[KvinLevelDb].nextIds.map(_.get()).toList

    // close and re-open store
    recreateStore

    val nextIdsLoaded = store.asInstanceOf[KvinLevelDb].nextIds.map(_.get()).toList
    assertEquals(nextIds, nextIdsLoaded)
  }
}