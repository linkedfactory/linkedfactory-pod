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
package io.github.linkedfactory.service.util

import io.github.linkedfactory.kvin.{Kvin, KvinTuple, Record}
import net.enilink.komma.core.{URI, URIs}
import net.liftweb.common.Box.box2Iterable
import net.liftweb.common._
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._

import javax.xml.datatype.DatatypeFactory

/**
 * Parses JSON objects with linked factory item data.
 */
object JsonFormatParser extends Loggable {
  val dtFactoryLocal = new ThreadLocal[DatatypeFactory]

  def datatypeFactory = {
    var factory = dtFactoryLocal.get
    if (factory == null) {
      factory = DatatypeFactory.newInstance
      dtFactoryLocal.set(factory)
    }
    factory
  }

  def parseItem(rootItem: URI, json: JValue, currentTime: Long = System.currentTimeMillis): Box[List[KvinTuple]] = {
    var activeContexts = List[JValue]()

    def collectErrors(a: Box[List[KvinTuple]], b: Box[List[KvinTuple]]): Box[List[KvinTuple]] = {
      (a, b) match {
        // accumulate errors
        case (a: Failure, b: Failure) => Failure(a.msg + "\n" + b.msg)
        case (a: Failure, b) => a
        case (a, b: Failure) => b
        // this is the cause for foldRight, foldLeft would always required to traverse
        // all previously folded values when using the ++ operator
        case (a, b) => Full(a.openOr(Nil) ++ b.openOr(Nil))
      }
    }

    def objectToRecord(o: JObject): Record = o.obj.foldLeft(Record.NULL) { case (e, field) =>
      val property = resolveUri(field.name, activeContexts)
      parseValue(field.value) match {
        case Full(value) => e.append(new Record(property, value))
        case _ => e
      }
    }

    def parseValue(value: JValue): Box[Any] = value match {
      case null | JNothing | JArray(_) =>
        Failure("Invalid value")
      case obj: JObject =>
        obj \ "@id" match {
          case JString(id) => Full(resolveUri(id, activeContexts))
          case _ => Full(objectToRecord(obj))
        }
      case value =>
        val unboxed = value.values
        Full(unboxed)
    }

    def parseProperty(item: URI, property: URI, values: List[JValue], currentTime: Long): Box[List[KvinTuple]] = {
      var generatedSeqNr = -1
      val result = values.map {
        // { "time" : 123, "seqNr" : 2, "value" : 1.3 }
        case JObject(fields) =>
          var seqNr = fields \ "seqNr" match {
            case JInt(n) => n.intValue
            case _ => 0
          }

          val time = (fields \ "time").toOpt.getOrElse(fields \ "t") match {
            case JString(s) => datatypeFactory.newXMLGregorianCalendar(s).toGregorianCalendar.getTimeInMillis
            case JInt(n) => n.longValue
            case _ =>
              if (seqNr == 0) {
                // generate sequence numbers for multiple values if neither time nor sequence numbers are specified
                generatedSeqNr += 1
                seqNr = generatedSeqNr
              }
              currentTime
          }

          parseValue((fields \ "value").toOpt.getOrElse(fields \ "v")) match {
            case Full(value) =>
              Full(new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, time, seqNr, value))
            case _ =>
              Failure("Invalid value for item \"" + item + "\" and property \"" + property + "\".")
          }
        case other => parseValue(other) match {
          case Full(value) =>
            Full(new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, currentTime, value))
          case _ =>
            Failure("Invalid value for item \"" + item + "\" and property \"" + property + "\".")
        }
      }

      result.foldRight(Empty: Box[List[KvinTuple]]) {
        // accumulate errors
        case (a: Failure, b: Failure) => Failure(a.msg + "\n" + b.msg)
        case (a: Failure, _) => a
        case (_, b: Failure) => b
        // this is the cause for foldRight, foldLeft would always required to traverse
        // all previously folded values when using the ++ operator
        case (a, b) => Full(a.toList ++ b.openOr(Nil))
      }
    }

    def resolveUri(uri: String, contexts: List[JValue]): URI = {
      uri.split(":") match {
        // is a URI with scheme
        case Array(_, suf, _*) if suf.startsWith("//") => URIs.createURI(uri)
        // may be a CURIE
        case Array(pref, _*) => contexts match {
          case first :: rest =>
            first \ pref match {
              case JString(s) =>
                val sufPref = resolveUri(s, contexts).toString
                if (pref.length < uri.length)
                  URIs.createURI(sufPref.concat(uri.substring(pref.length + 1)))
                else
                  URIs.createURI(sufPref.concat(uri.substring(pref.length)))
              case _ => resolveUri(uri, rest)
            }
          // no prefix defined, just use item as URI
          case Nil => {
            val result = URIs.createURI(uri)
            if (result.isRelative) result.resolve(rootItem) else result
          }
        }
      }
    }

    json match {
      // [ { "time" : 123, "seqNr" : 2, "value" : 1.3 } ]
      case JArray(values) =>
        parseProperty(rootItem, URIs.createURI("value"), values, currentTime)

      // { "item" : { "property1" : [ { "time" : 123, "seqNr" : 2, "value" : 1.3 } ], "property2" : [ { "time" : 123, "seqNr" : 5, "value" : 3.2 } ] } }
      case JObject(fields) => fields.flatMap {
        case JField(item, itemData) if item.equals("@context") => activeContexts = itemData :: activeContexts; None
        // "item" : { ... }
        case JField(item, itemData) if !item.equals("@context") =>
          // resolve relative URIs
          var itemUri = resolveUri(item, activeContexts)
          if (itemUri.lastSegment == "") itemUri = itemUri.trimSegments(1)
          itemData match {
            // "property1" : [{ ... }]
            case JObject(props) =>
              props.map {
                case JField(prop, propData) =>
                  // support single and multiple values
                  val values = propData match {
                    case JArray(values) => values
                    case other => List(other)
                  }
                  parseProperty(itemUri, resolveUri(prop, activeContexts), values, currentTime)
              }
            case _ => Failure("Invalid data: Expected an object with property keys.")
          }
      }.foldRight(Empty: Box[List[KvinTuple]])(collectErrors _)
      case _ => Failure("Invalid data")
    }
  }
}