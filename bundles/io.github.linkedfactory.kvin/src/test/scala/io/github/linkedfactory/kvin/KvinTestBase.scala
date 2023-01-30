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
package io.github.linkedfactory.kvin

import net.enilink.komma.core.URIs
import org.junit.Assert._
import org.junit.Test

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import scala.jdk.CollectionConverters._
import scala.util.Random

/**
 * Tests for the LevelDB-based time series store.
 */
abstract class KvinTestBase {
  val valueProperty = URIs.createURI("property:value")
  val seed = 200
  var storeDirectory: File = _
  var store: Kvin = _

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
    });
  }

  @Test
  def testWriteMultiple {
    val nrs = Array.fill(10)(Random.nextInt(Integer.MAX_VALUE))
    val count = 1000
    var rand = new Random(seed)
    for (i <- 0 to count) {
      val randomNr = nrs(rand.nextInt(nrs.length))
      val uri = URIs.createURI("http://example.org/item-" + randomNr + "/subitem/subsubitem-" + randomNr + "/measured-point-1")

      val time = Math.abs(rand.nextInt)
      val value = rand.nextInt
      store.put(new KvinTuple(uri, valueProperty, null, time, value))
    }
    rand = new Random(seed)
    for (i <- 0 to count) {
      val randomNr = nrs(rand.nextInt(nrs.length))
      val uri = URIs.createURI("http://example.org/item-" + randomNr + "/subitem/subsubitem-" + randomNr + "/measured-point-1")

      val time = Math.abs(rand.nextInt)
      val expected = rand.nextInt
      val r = store.fetch(uri, valueProperty, null, time, time, 1, 0, null)

      assertEquals(expected, r.toList.asScala.head.value)
    }
  }

  def itemUri(nr: Int) = URIs.createURI("http://example.org/l1/l2/item-" + nr + "/measure")

  def propertyUri(nr: Int) = URIs.createURI("http://example.org/p" + nr)

  def addData(items: Int, values: Int) {
    var rand = new Random(seed)
    for (nr <- 1 to items) {
      val uri = itemUri(nr)
      for (i <- 1 to values) {
        val time = Math.abs(rand.nextInt)
        val value = rand.nextInt
        store.put(new KvinTuple(uri, valueProperty, null, time, value))
      }
    }
  }

  @Test
  def testProperties {
    var rand = new Random(seed)
    for (itemNr <- 1 to 10) {
      val uri = itemUri(itemNr)
      for (propertyNr <- 1 to 5) {
        val pUri = propertyUri(propertyNr)
        for (i <- 1 to 10) {
          val time = Math.abs(rand.nextInt)
          val value = rand.nextInt
          store.put(new KvinTuple(uri, pUri, null, time, value))
        }
      }
    }

    val item4 = itemUri(4)
    val properties = (1 to 5).map(propertyUri(_)).toSet
    assertEquals(properties, store.properties(item4).toList.asScala.toSet)
  }

  @Test
  def testLimit {
    addData(3, 50)
    var limit = 5
    assertEquals(limit, store.fetch(itemUri(1), valueProperty, null, limit).toList.size)
    limit = 1
    assertEquals(limit, store.fetch(itemUri(1), valueProperty, null, limit).toList.size)
  }

  //@Test
  def testDelete {
    val valueCount = 5
    addData(3, valueCount)

    // exactly one item should exist
    assertEquals(1, store.descendants(itemUri(1)).toList.size)
    // item should have values
    assertEquals(valueCount, store.fetch(itemUri(1), valueProperty, null, 0).toList.size)

    store.delete(itemUri(1))

    // the number of values should be 0
    assertEquals(0, store.fetch(itemUri(1), valueProperty, null, 5).toList.size)
    // the item should not exist
    assertEquals(0, store.descendants(itemUri(1)).toList.size)
  }

  @Test
  def testInterval {
    var rand = new Random(seed)
    val points = 100
    val pointDistance: Long = 100
    val startTime: Long = pointDistance * points
    for (nr <- 1 to 2) {
      val uri = itemUri(nr)
      for (i <- 0 until points) {
        val time = startTime - i * pointDistance
        val value = rand.nextInt
        store.put(new KvinTuple(uri, valueProperty, null, time, value))
      }
    }

    var interval = 2 * pointDistance
    assertEquals(2, store.fetch(itemUri(1), valueProperty, null, startTime, startTime - interval, 0, interval, null).toList.size)

    val entries = store.fetch(itemUri(1), valueProperty, null, startTime, startTime - 2 * interval, 0, interval, null).toList
    assertEquals(3, entries.size)
    assertEquals(startTime, entries.get(0).time)
    assertEquals(startTime - interval, entries.get(1).time)
    assertEquals(startTime - 2 * interval, entries.get(2).time)
  }
}
