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
package io.github.linkedfactory.kvin.benchmark

import io.github.linkedfactory.kvin.{Kvin, KvinTuple}
import net.enilink.komma.core.URIs

import scala.jdk.CollectionConverters._
import scala.util.Random

/**
 * Simple benchmark for the LevelDB-based time series store.
 */
abstract class KvinBenchmarkBase extends App {
  def createStore: Kvin

  val store = createStore
  val valueProperty = URIs.createURI("property:value")
  val seed = 200
  val writeValues = 5000000
  val readValues = 2000

  var benchmarkStart = System.currentTimeMillis

  val startTimeValues = 1478252048736L
  try {
    val nrs = Array.fill(100)(Random.nextInt(Integer.MAX_VALUE))
    var rand = new Random(seed)

    var currentTime = startTimeValues
    (0 to writeValues).foreach { i =>
      if (i % 10000 == 0) println("  at: " + i)
      
      val randomNr = nrs(rand.nextInt(nrs.length))
      val uri = URIs.createURI("http://linkedfactory.github.io/" + randomNr + "/e3fabrik/rollex/" + randomNr + "/measured-point-1")
      val ctx = URIs.createURI("ctx:" + randomNr)

      val value = if (randomNr % 2 == 0) rand.nextGaussian else rand.nextLong(100000)

      store.put(new KvinTuple(uri, valueProperty, ctx, currentTime, value))
      currentTime += rand.nextInt(1000)
    }

    var seconds = (System.currentTimeMillis - benchmarkStart) / 1000.0
    println(s"Wrote $writeValues in %1$$,.2f seconds: %2$$,.2f ops per second".format(seconds, writeValues / seconds))
    
    rand = new Random(seed)
    benchmarkStart = System.currentTimeMillis
    val startTimeFetch = currentTime
    for (i <- 0 to readValues) {
      val randomNr = nrs(rand.nextInt(nrs.length))
      val uri = URIs.createURI("http://linkedfactory.github.io/" + randomNr + "/e3fabrik/rollex/" + randomNr + "/measured-point-1")
      val ctx = URIs.createURI("ctx:" + randomNr)

      val r = store.fetch(uri, valueProperty, ctx, startTimeFetch, startTimeValues, 2, 0, null).toList
      if (i % 1000 == 0) println(r)
    }

    println(s"Reading $readValues values took: " + String.format("%1$,.2f seconds", ((System.currentTimeMillis - benchmarkStart) / 1000.0).asInstanceOf[AnyRef]))

    store.descendants(URIs.createURI("")).toList.asScala.foreach { x => println("D: " + x) }
  } finally {
    Thread.sleep(5000)
    store.close()
  }
}
