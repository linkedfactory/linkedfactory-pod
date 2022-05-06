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
package io.github.linkedfactory.kvin.test

import io.github.linkedfactory.kvin.{Kvin, KvinTuple}
import io.github.linkedfactory.kvin.influxdb.KvinInfluxDb
import net.enilink.komma.core.URIs
import org.junit.Assert._
import org.junit.runners.MethodSorters
import org.junit.{After, Before, FixMethodOrder, Test}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class KvinInfluxDbTest {
  var db: KvinInfluxDb = _

  @Before
  def createStore {
    try {
      db = new KvinInfluxDb(URIs.createURI("http://localhost:8086"), "FoFab")
    } catch {
      case ex: Throwable => System.err.println("Local InfluxDB is unavailable. Skipping test.")
    }

    // execute test only if db could be initialized
    org.junit.Assume.assumeNotNull(db)
  }

  @Test
  def test0Version {
    val ver = db.influxdb.version()
    assertEquals(ver, db.ver)
  }

  @Test
  def test1Put {
    val numOfPuts = 4000
    val item = URIs.createURI("http://test.de/maschine1")
    val prop = URIs.createURI("http://test.de/activePower")
    val item2 = URIs.createURI("http://test.de/maschine2")
    val r = scala.util.Random
    var i = 0
    var time = 0
    for (i <- 1 to numOfPuts) {
      db.put(new KvinTuple(item, prop, Kvin.DEFAULT_CONTEXT, time, r.nextInt))
      time += 1
      db.put(new KvinTuple(item2, prop, Kvin.DEFAULT_CONTEXT, time, r.nextInt))
    }
    val res = db.getQuery("SELECT count(value) FROM \"" + prop.toString() + "\"")
    val number = res.getResults.get(0).getSeries.get(0).getValues.get(0).get(1).toString().toDouble
  }

  @Test
  def test2Get {
    val item = URIs.createURI("http://test.de/maschine1")
    val prop = URIs.createURI("http://test.de/activePower")
    val res = db.get(prop, item, System.currentTimeMillis() - 1000000000, System.currentTimeMillis())
  }

  @Test
  def test3fetch {
    val item = URIs.createURI("http://test.de/maschine8")
    val prop = URIs.createURI("http://test.de/activePower")
    val res = db.fetch(item, prop, Kvin.DEFAULT_CONTEXT, 100).iterator
    while (res.hasNext()) {
      val curr = res.next()
      println("Fetchtest: " + curr.value.toString())
    }

  }

  @Test
  def test5properties {
    val item = URIs.createURI("http://test.de/maschine1")
    val res = db.properties(item)
    val test = res.toList().iterator
    while (test.hasNext()) {
      val curr = test.next()
      //println("property: "+curr.toString())
    }
  }

  @Test
  def test4fetchWithInterval {
    val item = URIs.createURI("http://test.de/maschine1")
    val prop = URIs.createURI("http://test.de/activePower")
    val res = db.fetch(item, prop, Kvin.DEFAULT_CONTEXT, System.currentTimeMillis(), System.currentTimeMillis() - 1000000000, 30000, 20).iterator
    while (res.hasNext()) {
      val curr = res.next()
      //println("test" + curr.value)
    }
  }

  @Test
  def test6decendant {
    val item = URIs.createURI("http://test.de/maschine")
    val res = db.descendants(item, 10)
    val test = res.toList().iterator
    while (test.hasNext()) {
      val curr = test.next()
      //println("testvalue: "+curr.toString())
    }
  }

  @Test
  def test7approximateSize {
    val item = URIs.createURI("http://test.de/maschine1")
    val prop = URIs.createURI("http://test.de/activePower")
    val res = db.approximateSize(item, prop, Kvin.DEFAULT_CONTEXT, System.currentTimeMillis(), System.currentTimeMillis() - 1000000000)
    //println("Size: "+res)
  }

  @Test
  def test8Delete {
    val item = URIs.createURI("http://test.de/maschine1")
    val prop = URIs.createURI("http://test.de/activePower")
    db.delete(item)
    db.delete(item, prop, Kvin.DEFAULT_CONTEXT, Long.MaxValue / 1000000, 0L)
  }

  @After
  def closeStore {
    if (db != null) db.close
  }
}
