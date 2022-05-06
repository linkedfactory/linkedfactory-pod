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
package io.github.linkedfactory.service

import net.liftweb.actor.LiftActor
import net.liftweb.common.Loggable
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import net.liftweb.util.Schedule
import org.apache.http.client.fluent.Request
import org.apache.http.entity.{ContentType, StringEntity}

import java.util.concurrent.{Future, ScheduledFuture}
import java.util.{Date, GregorianCalendar}
import javax.xml.datatype.DatatypeFactory
import scala.collection.mutable
import scala.util.Random

class MockingDataActor(serverUrl: String, machines: Int) extends LiftActor with Loggable {
  implicit val formats = net.liftweb.json.DefaultFormats

  @volatile var future: ScheduledFuture[_] = _
  val states: mutable.Map[String, Double] = mutable.Map.empty

  def start { Schedule.schedule(this, (), 1000L) }

  def stop {
    future match {
      case f: Future[_] => f.cancel(true)
      case _ => // future not yet created
    }
  }

  override def messageHandler: PartialFunction[Any, Unit] = {
    case _ => {
      future = Schedule.schedule(this, (), 3000L)

      val c = new GregorianCalendar
      c.setTime(new Date)
      val calendar = DatatypeFactory.newInstance.newXMLGregorianCalendar(c)
      val time = calendar.toString

      if (machines > 0) postSensorDataToValueStore(time)
    }
  }

  private def generateValue(name: String): Double = {
    def randomState = 2.0 + 2 * Random.nextInt(4)
    var state = states.getOrElseUpdate(name, randomState)
    val offset = 1.0 / (3 + Random.nextInt(5))
    if (Random.nextDouble < .03) {
      state = randomState
      states.put(name, state)
    }
    state + offset
  }

  private def postSensorDataToValueStore(time: String): Unit = {
    val postPath = "linkedfactory/demofactory/values"

    val fields = (1 to machines).flatMap { m =>
      (for (i <- 1 to 10) yield {
        val name = "machine" + m + "/sensor" + i
        JField(name,
          ("value", ("time", time) ~ ("value", decompose(generateValue(name + "$value")))) ~
            ("flag", ("time", time) ~ ("value", decompose(Random.nextBoolean()))) ~
            ("a", ("time", time) ~ ("value", decompose(generateValue(name + "$a")))) ~
            ("a", ("time", time) ~ ("value", decompose(generateValue(name + "$b")))) ~
            ("json", ("time", time) ~ ("value",
              ("status", decompose(generateValue(name + "$status"))) ~
                ("message", decompose("Message_" + Random.nextInt(5)))
            ))
        )
      }).toList :+ {
        val name = "machine" + m
        JField(name, ("value", ("time", time) ~ ("value", decompose(generateValue(name)))))
      }
    }.toList

    val postBody = compactRender(JObject(fields))
    val response = Request.Post(serverUrl + postPath).body(new StringEntity(postBody, ContentType.APPLICATION_JSON)).execute.returnResponse
    if (response.getStatusLine.getStatusCode != 200) logger.error("Posting data failed with code: " + response.getStatusLine.getStatusCode)
  }
}