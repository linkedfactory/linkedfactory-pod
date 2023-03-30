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

import io.github.linkedfactory.kvin.rocksdb.KvinRocksDb
import net.enilink.komma.core.URIs
import org.junit.Assert._
import org.junit.{After, Before, Test}

import java.io.File
import scala.util.Random

/**
 * Tests for the LevelDB-based time series store.
 */
class KvinRocksDbTest extends KvinTestBase {
  @Before
  def createStore {
    storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis + "-" + Random.nextInt(1000) + "/")
    storeDirectory.deleteOnExit
    store = new KvinRocksDb(storeDirectory)
  }

  @After
  def closeStore {
    store.close
    store = null
    deleteDirectory(storeDirectory.toPath)
  }
  
  @Test
  def testEvents {
    val ctx = null
    
    var rand = new Random(seed)
    val points = 100
    val pointDistance: Long = 100
    val startTime: Long = pointDistance * points
    for (nr <- 1 to 2) {
      val uri = itemUri(nr)
      for (i <- 0 until points) {
        val time = startTime - i * pointDistance
        val value = new Record(URIs.createURI("prop:itemNr"), nr).append(new Record(URIs.createURI("prop:pointNr"), i)).append(new Record(URIs.createURI("prop:value"), URIs.createURI("some:value")))
        // println("ADD: " + new KvinTuple(uri, valueProperty, ctx, time, value))
        store.put(new KvinTuple(uri, valueProperty, ctx, time, value))
      }
    }
    
    var interval = 2 * pointDistance
    // println(store.fetch(itemUri(1), valueProperty, ctx, startTime, startTime - interval, 0, 0, null).toList)

    assertEquals(3, store.fetch(itemUri(1), valueProperty, ctx, startTime, startTime - interval, 0, 0, null).toList.size)
  }
}