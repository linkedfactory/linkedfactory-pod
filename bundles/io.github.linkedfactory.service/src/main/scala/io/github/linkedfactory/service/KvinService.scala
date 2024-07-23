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

import io.github.linkedfactory.core.kvin.util.CsvFormatParser
import io.github.linkedfactory.core.kvin.{Kvin, KvinTuple, Record}
import io.github.linkedfactory.service.util.{JsonFormatParser, LineProtocolParser}
import net.enilink.commons.iterator.IExtendedIterator
import net.enilink.komma.core.{URI, URIs}
import net.liftweb.common.Box.box2Iterable
import net.liftweb.common._
import net.liftweb.http.rest.RestHelper
import net.liftweb.http.{BadRequestResponse, InMemoryResponse, JsonResponse, LiftResponse, OkResponse, OutputStreamResponse, PlainTextResponse, Req, S}
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import net.liftweb.util.Helpers._
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import java.io.{InputStream, OutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KvinService(path: List[String], store: Kvin) extends RestHelper with Loggable {
  val MAX_LIMIT = 500000
  val valueProperty: URI = URIs.createURI("value")

  val CORS_HEADERS: List[(String, String)] = ("Access-Control-Allow-Origin", "*") :: ("Access-Control-Allow-Credentials", "true") :: //
    ("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS") :: //
    ("Access-Control-Allow-Headers", "WWW-Authenticate,Keep-Alive,User-Agent,X-Requested-With,Cache-Control,Content-Type") :: Nil

  def responseHeaders: List[(String, String)] = CORS_HEADERS ::: S.getResponseHeaders(Nil)

  object FailureResponse {
    def apply(msg: String): PlainTextResponse = PlainTextResponse(msg, Nil, 400)
  }

  protected def csvResponse_?(r: Req): Boolean = {
    S.param("type").exists(_ == "text/csv") ||
      (r.weightedAccept.exists(_.matches("text" -> "csv")) && !r.acceptsStarStar) ||
      (r.weightedAccept.isEmpty || r.acceptsStarStar) && r.path.suffix.equalsIgnoreCase("csv")
  }

  protected def responseType(r: Req): Box[String] = {
    S.param("type") or {
      if (jsonResponse_?(r)) Full("application/json")
      else if (csvResponse_?(r)) Full("text/csv")
      else Empty
    }
  }

  def createJsonResponse(json: JValue): LiftResponse = JsonResponse(json, responseHeaders, S.responseCookies, 200)

  serve(path prefix {
    // support OPTIONS requests
    case list Options req if list.endsWith("values" :: Nil) || list.endsWith("properties" :: Nil) || //
      list.endsWith("**" :: Nil) || list.endsWith("values" :: "size" :: Nil) =>
      InMemoryResponse(Array(), responseHeaders, S.responseCookies, 200)
    case list Post req if list.endsWith("values" :: Nil) =>
      val result = req.contentType match {
        case Full("application/influxdb-line") =>
          req.rawInputStream.flatMap(saveLineValues(_, path ++ list.dropRight(1), System.currentTimeMillis))
        case Full("text/csv") =>
          req.rawInputStream.flatMap(saveCsvValues(_, path ++ list.dropRight(1), System.currentTimeMillis))
        case _ =>
          req.json.flatMap(saveValues(_, path ++ list.dropRight(1), System.currentTimeMillis))
          // req.rawInputStream.flatMap(saveValues(_, path ++ list.dropRight(1), System.currentTimeMillis))
      }
      result match {
        case Failure(msg, _, _) => FailureResponse(msg)
        case _ => OkResponse()
      }
    case list Get req if list.endsWith("values" :: Nil) =>
      val limit = S.param("limit") flatMap (v => tryo(v.toLong)) filter (_ > 0) openOr 10000L

      if (limit > MAX_LIMIT) {
        FailureResponse("The maximum limit is " + MAX_LIMIT + ". Please use multiple request if you require more data points.")
      } else {
        val values = getValues(path ++ list.dropRight(1), limit)

        def filename(defaultExt: String) = S.param("filename") openOr "values." + defaultExt

        val formatDate: Long => String = {
          S.param("dateformat").map { f =>
            val timezoneoffset = S.param("timezoneoffset").map(_.toInt) openOr 0
            val formatter = new SimpleDateFormat(f)
            (timestamp: Long) => {
              val adjustedTime = timestamp - 60000 * timezoneoffset
              formatter.format(new Date(adjustedTime))
            }
          } openOr {
            // the following does not work for some reason with Scala 2.13
            // (ts : Long) => ts.toString
            def toString(ts: Long) = ts.toString
            toString _
          }
        }

        def recordToJson(r : Record) : JObject = JObject(r.iterator().asScala.map { e =>
          JField(e.getProperty.toString, e.getValue match {
            case r : Record => recordToJson(r)
            case uri : URI => JObject(JField("@id", uri.toString))
            case other => decompose(other)
          })
        }.toList)

        // fast path to avoid value->JSON->string conversion for simple types
        def value2Str(value: Any, quoteStrings: Boolean = true) = value match {
          case null => "null"
          case b: Boolean => b.toString
          case n: Number => n.toString
          case uri : URI => compactRender(JObject(JField("@id", uri.toString)))
          case x: JValue => compactRender(x)
          case e: Record => compactRender(recordToJson(e))
          case other if quoteStrings => compactRender(JString(other.toString))
          case other => other.toString
        }

        val response = responseType(req) map {
          case "application/json" =>
            // { "item" : { "property1" : [ { "time" : 123, "seqNr" : 2, "value" : 1.3 } ], "property2" : [ { "time" : 123, "seqNr" : 5, "value" : 3.2 } ] } }
            val streamer = (os: OutputStream) => {
              val streamWriter = new OutputStreamWriter(os)
              // open JSON result object, then iterate over the items
              streamWriter.write("{")
              for (((item, itemData), i) <- values.view.zipWithIndex) {
                // open item object, then iterate over the properties
                streamWriter.write(s"""\n  "$item":{""")
                for (((property, propertyData), p) <- itemData.view.zipWithIndex) {
                  // open array of property values, then fully consume the iterator
                  streamWriter.write(s"""\n    "$property":[""")
                  for ((entry, k) <- propertyData.iterator.asScala.zipWithIndex) {
                    if (propertyData.hasNext && k % 100 == 0) streamWriter.write("\n      ")
                    streamWriter.write(s"""{"time":${entry.time},"seqNr":${entry.seqNr},"value":${ value2Str(entry.value) }}""")
                    if (propertyData.hasNext) streamWriter.write(",")
                  }
                  propertyData.close() // close the iterator
                  streamWriter.write("\n    ]") // close array of property values
                  if (p < itemData.size - 1) streamWriter.write(",")
                }
                streamWriter.write("\n  }") // close item object
                if (i < values.size - 1) streamWriter.write(",")
              }
              streamWriter.write("\n}") // close JSON result object
              streamWriter.close()
            }
            OutputStreamResponse(streamer, -1, ("Content-Type", "application/json; charset=utf-8") ::
              ("Content-Disposition", s"""inline; filename=${filename("json")}""") :: responseHeaders, S.responseCookies, 200)
          case "text/csv" =>
            // { "item" : { "property1" : [ { "time" : 123, "seqNr" : 2, "value" : 1.3 } ], "property2" : [ { "time" : 123, "seqNr" : 5, "value" : 3.2 } ] } }
            val streamer = (os: OutputStream) => {
              val streamWriter = new OutputStreamWriter(os)
              val csvFormat = CSVFormat.EXCEL
              val csvPrinter = new CSVPrinter(streamWriter, csvFormat)

              // either use supplied properties or properties retrieved from database
              val propertiesParam = S.param("properties").map(_.split("\\s+").toList)

              val itemProperties = values.map(v => {
                (propertiesParam openOr v._2.map(_._1).toSet.toList.sorted).map((v._1, _))
              }).flatten.toList

              // print header row
              csvPrinter.printRecord(("time" :: itemProperties.map(p => s"<${p._1}>@<${p._2}>")).asJava)

              var itemData = values.map(v => {
                val ps = propertiesParam openOr v._2.map(_._1).toSet.toList.sorted
                ps.flatMap(p => v._2.get(p).map(it => (if (it.hasNext()) it.next() else null, it)))
              }).flatten

              val ordering: Ordering[KvinTuple] = (a: KvinTuple, b: KvinTuple) => {
                // compare first by time and then by seqNr
                val diffTime = a.time - b.time
                if (diffTime != 0) diffTime.toInt else a.seqNr - b.seqNr
              }
              var finished = false
              while (!finished) {
                val nextTuples = itemData.map(_._1).filter(_ != null)
                val maxTuple = if (nextTuples.isEmpty) null else nextTuples.max(ordering)
                if (maxTuple != null) {
                  // print the row, properties without values at row timestamp stay unset
                  csvPrinter.printRecord((formatDate(maxTuple.time) :: itemData.map(d => {
                    if (d._1 != null && ordering.compare(maxTuple, d._1) == 0 && d._1.value != null) value2Str(d._1.value, false) else null
                  }).toList).asJava)

                  // select next tuple from iterators
                  itemData = itemData.map(d => {
                    if (d._1 != null && ordering.compare(maxTuple, d._1) == 0) {
                      val it = d._2
                      val next = if (it.hasNext()) it.next() else {
                        it.close()
                        null
                      }
                      (next, if (next != null) it else null)
                    } else d
                  })
                } else {
                  finished = true
                }
              }

              csvPrinter.close()
            }
            OutputStreamResponse(streamer, -1, ("Content-Type", "text/csv; charset=utf-8") ::
              ("Content-Disposition", s"""inline; filename=${filename("csv")}""") :: responseHeaders, S.responseCookies, 200)
          case _ => BadRequestResponse()
        } openOr BadRequestResponse()
        response
      }
    case list Get _ if list.endsWith("properties" :: Nil) => createJsonResponse(getProperties(path ++ list.dropRight(1)))
    case list Get _ if list.endsWith("**" :: Nil) => createJsonResponse(getDescendants(path ++ list.dropRight(1)))

    case list Delete _ if list.endsWith("values" :: Nil) => createJsonResponse(deleteValues(path ++ list.dropRight(1)))
    // case list Get _ => // TODO return RDF description
  })

  // handle JSON post content
  def saveValues(json: JValue, path: List[String], currentTime: Long): Box[_] = {
    var parentUri = Data.pathToURI(path)
    if (parentUri.lastSegment != "") parentUri = parentUri.appendSegment("")

    JsonFormatParser.parseItem(parentUri, json, currentTime) map ( _.foreach { tuple =>
        store.put(tuple)
    })
  }

  // handle JSON post content
  def saveValues(in: InputStream, path: List[String], currentTime: Long): Box[_] = {
    var parentUri = Data.pathToURI(path)
    if (parentUri.lastSegment != "") parentUri = parentUri.appendSegment("")

    try {
      val tuples : IExtendedIterator[KvinTuple] = new io.github.linkedfactory.core.kvin.util.JsonFormatParser(in).parse(currentTime)
      store.put(tuples)
      Empty
    } catch {
      case e : Exception => new Failure(e.getMessage(), Full(e), Empty)
    }
  }

  // handle CSV post content
  def saveCsvValues(in: InputStream, path: List[String], currentTime: Long): Box[_] = {
    var parentUri = Data.pathToURI(path)
    if (parentUri.lastSegment != "") parentUri = parentUri.appendSegment("")

    try {
      val separator = S.param("separator").map(_.trim).filter(_.nonEmpty).map(_.charAt(0)).getOrElse(',')
      val tuples : IExtendedIterator[KvinTuple] = new CsvFormatParser(parentUri, separator, in).parse()
      store.put(tuples)
      Empty
    } catch {
      case e : Exception => new Failure(e.getMessage(), Full(e), Empty)
    }
  }

  // handle InfluxDB line protocol content
  def saveLineValues(is: InputStream, path: List[String], currentTime: Long): Box[_] = {
    var parentUri = Data.pathToURI(path)
    if (parentUri.lastSegment != "") parentUri = parentUri.appendSegment("")

    LineProtocolParser.parseLines(parentUri, is, currentTime) map ( _.foreach { tuple =>
        store.put(tuple)
    })
  }

  def getSingleItem(path: List[String]): URI = S.param("item") flatMap { s => tryo(URIs.createURI(s)) } openOr Data.pathToURI(path)

  def getValues(path: List[String], limit: Long): Map[String, Map[String, IExtendedIterator[KvinTuple]]] = {
    val items = (S.param("item") or S.param("items")).map {
      _.split("\\s+").flatMap { i => tryo(URIs.createURI(i)) }.toList
    } openOr List(Data.pathToURI(path))

    val end = S.param("to") flatMap (v => tryo(v.toLong)) openOr KvinTuple.TIME_MAX_VALUE
    val begin = S.param("from") flatMap (v => tryo(v.toLong)) openOr 0L

    // allow also fractional intervals
    val interval = S.param("interval") flatMap (v => tryo(v.toDouble.longValue)) openOr 0L
    val op = S.param("op") map (_.trim)

    val results = items map { item =>
      val itemData = for (
        property <- {
          (S.param("property") or S.param("properties")).map {
            _.split("\\s+").flatMap { s => tryo(URIs.createURI(s)) }.toList
          } openOr store.properties(item).toList.asScala
        }
      ) yield {
        val propertyData = (interval, op) match {
          case (_, Full(op)) if interval > 0 =>
            store.fetch(item, property, Kvin.DEFAULT_CONTEXT, end, begin, limit, interval, op)
          case _ =>
            store.fetch(item, property, Kvin.DEFAULT_CONTEXT, end, begin, limit, 0, null)
        }
        property.toString -> propertyData
      }
      item.toString -> itemData.toMap
    }
    results.toMap
  }

  def deleteValues(path: List[String]): JObject = {
    val items = (S.param("item") or S.param("items")).map {
      _.split("\\s+").flatMap { i => tryo(URIs.createURI(i)) }.toList
    } openOr List(Data.pathToURI(path))

    val end = S.param("to") flatMap (v => tryo(v.toLong)) openOr Long.MaxValue
    val begin = S.param("from") flatMap (v => tryo(v.toLong)) openOr 0L

    val deletedRows = items.foldLeft(0L) {
      case (count: Long, item: URI) =>
        val properties = {
          (S.param("property") or S.param("properties")).map {
            _.split("\\s+").flatMap { s => tryo(URIs.createURI(s)) }.toList
          } openOr store.properties(item).toList.asScala
        }
        count + properties.foldLeft(0L) {
          case (count2: Long, property: URI) =>
            count2 + store.delete(item, property, Kvin.DEFAULT_CONTEXT, end, begin)
        }
    }

    JObject(JField("deleted", deletedRows) :: Nil)
  }

  def getDescendants(path: List[String]): JArray = {
    val uri = path match {
      // retrieve all items if path is the root path
      case p if p == this.path && S.param("item").isEmpty => URIs.createURI("")
      case p => getSingleItem(p) match {
        case u if u.lastSegment != "" => u.appendSegment("")
        case u => u
      }
    }

    val descendants = store.descendants(uri).iterator.asScala.map {
      uri => JObject(JField("@id", uri.toString) :: Nil)
    }
    JArray(descendants.toList)
  }

  def getProperties(path: List[String]): JArray = {
    val uri = getSingleItem(path)
    val properties = store.properties(uri).iterator.asScala.map {
      uri => JObject(JField("@id", uri.toString) :: Nil)
    }
    JArray(properties.toList)
  }
}
