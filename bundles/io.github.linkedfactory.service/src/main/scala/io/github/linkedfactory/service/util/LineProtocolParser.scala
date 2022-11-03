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

import io.github.linkedfactory.kvin.Kvin
import io.github.linkedfactory.kvin.KvinTuple

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import net.enilink.komma.core.URI
import net.enilink.komma.core.URIs
import net.liftweb.common.Box
import net.liftweb.common.Empty
import net.liftweb.common.Failure
import net.liftweb.common.Full
import net.liftweb.common.Loggable
import net.liftweb.util.Helpers.tryo

/**
 * Parses InfluxDB line protocol with linked factory item data.
 * <p>
 * LF properties map to InfluxDB's measurements, LF items map to tags (key='item').
 */
object LineProtocolParser extends Loggable {
  def parseLines(rootItem: URI, is: InputStream, currentTime: Long = System.currentTimeMillis): Box[List[KvinTuple]] = {
    val reader = new BufferedReader(new InputStreamReader(is))
    val p = new Parser
    // FIXME: toList -> all read into memory
    val result = LazyList.continually(reader.readLine()).takeWhile(_ != null).filter(!_.isEmpty).toList.map { l =>
      tryo(p.parse(l, currentTime))
    }
    reader.close()
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

  def mkItemData(property: String, item: String, value: String, time: String, currentTime: Long = System.currentTimeMillis): KvinTuple = {
    new KvinTuple(
      URIs.createURI(item.trim),
      URIs.createURI(property.trim),
      Kvin.DEFAULT_CONTEXT,
      // Note: line protocol timestamp defaults to nanosecond precision
      if (time == null || time.trim.isEmpty) currentTime else time.trim.toLong / 1000 / 1000,
      value.trim match {
        case s: String if s.startsWith("\"") && s.endsWith("\"") => s.substring(1, s.length - 1)
        case i: String if i.endsWith("i") => Integer.valueOf(i.substring(0, i.length - 1))
        // see line protocol tutorial for the Boolean values
        case t: String if t == "t" || t == "T" || t == "true" || t == "True" || t == "TRUE" => true
        case f: String if f == "f" || f == "F" || f == "false" || f == "False" || f == "FALSE" => false
        case o @ _ => o.toDouble
      })
  }
}

/**
 * Simple Parser to handle line protocol entries for InfluxDB's line protocol.
 */
class Parser {
  def parse(input: CharSequence, currentTime: Long = System.currentTimeMillis): KvinTuple = {
    val t = new Tokenizer(input)
    class ParseException(val reason: String) extends Exception(reason + " @ " + t.pos + ": '" + t.remainder + "'")

    val tagSet = scala.collection.mutable.Map[String, String]()
    val fieldSet = scala.collection.mutable.Map[String, String]()

    val measurement = t.nextToken
    var n = t.nextToken
    if (n.isEmpty) throw new ParseException("Parse error: unexpected end of input after measurement!")
    // read the tag set (up to next whitespace)
    while (n.nonEmpty && n != " " && n != "\t") {
      val k = if (n == ",") t.nextToken else n
      // skip the '='
      if (t.nextToken != "=") throw new ParseException("Parse error: unexpected content after tag key '" + k + "'!")
      val v = t.nextToken
      tagSet += (k -> v)
      n = t.nextToken // ',' or whitespace
    }
    // read all remaining whitespace before the field set
    while (n == " " || n == "\t") {
      n = t.nextToken
    }
    if (n.isEmpty) throw new ParseException("Parse error: unexpected end of input after " + (if (tagSet.isEmpty) "measurement" else "tag set!"))
    // read the field set (up to next whitespace or EOI)
    while (n.nonEmpty && n != " " && n != "\t") {
      val k = if (n == ",") t.nextToken else n
      // skip the '='
      if (t.nextToken != "=") throw new ParseException("Parse error: unexpected content after field key '" + k + "'!")
      val v = t.nextToken
      fieldSet += (k -> v)
      n = t.nextToken
    }
    // read all remaining whitespace before timestamp (if any)
    while (n == " " || n == "\t") {
      n = t.nextToken
    }
    val timestamp = n // note: timestamp is optional and can be the empty string
    if (t.hasNextToken) throw new ParseException("Parse error: unexpected content after timestamp!")

    // FIXME: improve error message if tag 'item' or key 'value' are missing
    // FIXME: handle additional fields (and maybe tags as different items to update?)
    LineProtocolParser.mkItemData(measurement, tagSet("item"), fieldSet("value"), timestamp, currentTime)
  }
}

/**
 * Simple Tokenizer to handle escape characters and delimiters for InfluxDB's line protocol.
 */
class Tokenizer(val input: CharSequence) {
  protected val len: Int = input.length
  protected var curPos = 0

  def nextToken: String = {
    val tokenStart = curPos
    var tokenEnd = -1
    // flag: the last character was the escape char
    var escaped = false
    // list: positions of all escape characters
    val escapePos = new scala.collection.mutable.ListBuffer[Int]
    // stop when token was found or at EOI
    while (tokenEnd == -1 && curPos < len) {
      val curChar = input.charAt(curPos)
      if (!escaped && (curChar == '=' || curChar == ',' || curChar == ' ' || curChar == '\t')) {
        // un-escaped delimiter found, token ends here (but delimiter has been read!)
        tokenEnd = curPos
        // make sure to return delimiter char by itself on subsequent invocation
        // for token before: unread delimiter, for delimiter itself: include its char
        if (tokenEnd > tokenStart) curPos -= 1 else tokenEnd += 1
      }

      // update escaped flag for current char
      escaped = curChar == '\\'
      // keep track of all escape characters
      if (escaped) escapePos.append(curPos)

      // continue with next char
      curPos += 1
    }
    // ensure to consume tokens up to end of input
    if (curPos == len) tokenEnd = len

    // return token, drop all escape characters (if any)
    // use known positions to concatenate list of sub-sequences
    var s = tokenStart
    escapePos.append(tokenEnd)
    escapePos.foldLeft(new StringBuilder)( (b, e) => {
      // sub-sequence between prior and current escape char
      b.append(input.subSequence(s, e))
      s = e + 1 // next sub-sequence after current escape char
      b
    }).toString
  }

  def pos: Int = curPos

  def hasNextToken: Boolean = curPos < len

  def remainder: CharSequence = input.subSequence(curPos, len)
}
