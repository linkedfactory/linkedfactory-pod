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
import net.enilink.komma.core.{URI, URIs}
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
  def createStore(): Unit = {
    storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis + "-" + Random.nextInt(1000) + "/")
    storeDirectory.deleteOnExit()
    store = new KvinLevelDb(storeDirectory)
  }

  def recreateStore(): Unit = {
    store.close()
    store = new KvinLevelDb(storeDirectory)
  }

  @After
  def closeStore(): Unit = {
    store.close()
    store = null
    deleteDirectory(storeDirectory.toPath)
  }

  @Test
  def testRecordsParallel(): Unit = {
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
    recreateStore()

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
            record.first(URIs.createURI("prop:value")).getValue)
        }
    }
  }

  @Test
  def testIds(): Unit = {
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
    recreateStore()

    val nextIdsLoaded = store.asInstanceOf[KvinLevelDb].nextIds.map(_.get()).toList
    assertEquals(nextIds, nextIdsLoaded)
  }

  @Test
  def testBatchWriteSwitchesAndReusesPrefixesAcrossFlush(): Unit = {
    val item = itemUri(101)
    val otherItem = itemUri(102)
    val property = valueProperty
    val otherProperty = propertyUri(2)
    val context = URIs.createURI("http://example.org/context/one")
    val otherContext = URIs.createURI("http://example.org/context/two")
    val largeValue = "x".repeat(2200)

    // The first and last runs deliberately use the same series.  The middle
    // run changes every part of the series identity and the write crosses the
    // one-megabyte value-batch threshold while the cache is in use.
    val tuples =
      (0 until 260).map(i => new KvinTuple(item, property, context, i, i, largeValue)) ++
        Seq(new KvinTuple(otherItem, otherProperty, otherContext, 10000L, 0, largeValue)) ++
        (0 until 260).map(i => new KvinTuple(item, property, context, 20000L + i, i, largeValue))

    store.put(tuples.asJava)

    def fetched(item: URI, property: URI, context: URI): List[KvinTuple] =
      store.fetch(item, property, context, 0).toList.asScala.toList

    val firstSeries = fetched(item, property, context)
    assertEquals(520, firstSeries.size)
    assertEquals((0 until 260).toList ++ (20000 until 20260).toList, firstSeries.map(_.time).sorted)
    assertEquals(1, fetched(otherItem, otherProperty, otherContext).size)
    assertEquals(largeValue, firstSeries.head.value)

    recreateStore()

    assertEquals(520, fetched(item, property, context).size)
    assertEquals(1, fetched(otherItem, otherProperty, otherContext).size)
  }

  @Test
  def testBatchWriteRecoversAfterIteratorFailure(): Unit = {
    val item = itemUri(103)
    val tuple = new KvinTuple(item, valueProperty, Kvin.DEFAULT_CONTEXT, 1L, 0, "before failure")
    val failingEntries = new java.lang.Iterable[KvinTuple] {
      override def iterator(): java.util.Iterator[KvinTuple] = new java.util.Iterator[KvinTuple] {
        private var returned = false

        override def hasNext(): Boolean = {
          if (!returned) true
          else throw new IllegalStateException("iterator failure")
        }

        override def next(): KvinTuple = {
          returned = true
          tuple
        }

        override def remove(): Unit = throw new UnsupportedOperationException
      }
    }

    try {
      store.put(failingEntries)
      fail("Expected the source iterator to fail")
    } catch {
      case _: IllegalStateException =>
    }

    val afterFailure = new KvinTuple(item, valueProperty, Kvin.DEFAULT_CONTEXT, 2L, 0, "after failure")
    store.put(afterFailure)

    val values = store.fetch(item, valueProperty, Kvin.DEFAULT_CONTEXT, 0).toList.asScala
    assertEquals(1, values.size)
    assertEquals(afterFailure.time, values.head.time)
    assertEquals(afterFailure.value, values.head.value)
  }
}
