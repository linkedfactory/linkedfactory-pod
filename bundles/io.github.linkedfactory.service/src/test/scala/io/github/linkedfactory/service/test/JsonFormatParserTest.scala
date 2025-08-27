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
package io.github.linkedfactory.service.test

import io.github.linkedfactory.core.kvin.{Kvin, KvinTuple, Record}
import io.github.linkedfactory.service.util.JsonFormatParser
import net.enilink.komma.core.URIs
import net.liftweb.common.{Failure, Full}
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import org.junit.{Assert, Test}
import org.scalatest.Assertions.fail


class JsonFormatParserTest {

  @Test
  def test(): Unit = {
    val context = Kvin.DEFAULT_CONTEXT
    val time = System.currentTimeMillis
    val root = URIs.createURI("http://example.root")
    val simpleJson: JValue =
      ("@context" -> ("pref" -> "http://test1.example/")) ~
        ("pref" -> ("pref:rest" -> "val") ~ ("pref2:pref3" -> "val2"))
    var tuple = JsonFormatParser.parseItem(root, context, simpleJson, time).head.head
    Assert.assertEquals("http://test1.example/", tuple.item.toString)
    Assert.assertEquals("http://test1.example/rest", tuple.property.toString)
    Assert.assertEquals("val", tuple.value)
    tuple = JsonFormatParser.parseItem(root, context, simpleJson, time).head.tail.head
    Assert.assertEquals("http://test1.example/", tuple.item.toString)
    Assert.assertEquals("pref2:pref3", tuple.property.toString)
    Assert.assertEquals("val2", tuple.value)

    val withoutContext = ("pref" -> ("pref:rest" -> "val") ~ ("pref2:pref3" -> "val2"))
    tuple = JsonFormatParser.parseItem(root, context, withoutContext, time).head.head
    Assert.assertEquals("http://example.root/pref", tuple.item.toString)
    Assert.assertEquals("pref:rest", tuple.property.toString)
    Assert.assertEquals("val", tuple.value)

    val withMultiContext = ("@context" -> ("pref" -> "http://test1.example/") ~ ("pref2" -> "http://pref2.example/")) ~
      ("@context" -> ("pref" -> "http://test2.example/")) ~
      ("pref" -> ("pref:rest" -> "val") ~ ("pref2:rest" -> "val2"))
    tuple = JsonFormatParser.parseItem(root, context, withMultiContext, time).head.head
    Assert.assertEquals("http://test2.example/", tuple.item.toString)
    Assert.assertEquals("http://test2.example/rest", tuple.property.toString)
    Assert.assertEquals("val", tuple.value)
    tuple = JsonFormatParser.parseItem(root, context, withMultiContext, time).head.tail.head
    Assert.assertEquals("http://test2.example/", tuple.item.toString)
    Assert.assertEquals("http://pref2.example/rest", tuple.property.toString)
    Assert.assertEquals("val2", tuple.value)

    val prefixInContext = ("@context" -> ("pref" -> "http://test1.example/") ~ ("pref2" -> "pref:pref1")) ~
      ("@context" -> ("pref" -> "http://test2.example/")) ~
      ("pref" -> ("pref:rest" -> "val") ~ ("pref2" -> "val2"))
    tuple = JsonFormatParser.parseItem(root, context, prefixInContext, time).head.tail.head
    Assert.assertEquals(tuple.property.toString, "http://test1.example/pref1")

    val multiPrefixes = ("@context" -> ("pref" -> "http://test1.example/") ~ ("pref2" -> "pref:pref1")) ~
      ("pref:pref1/pref2" -> ("pref:rest" -> "val") ~ ("pref2" -> "val2"))
    tuple = JsonFormatParser.parseItem(root, context, multiPrefixes, time).head.head
    Assert.assertEquals(tuple.item.toString, "http://test1.example/pref1/pref2")
  }

  @Test
  def testNested(): Unit = {
    val context = Kvin.DEFAULT_CONTEXT
    val time = System.currentTimeMillis
    val root = URIs.createURI("http://example.root")
    val nested = "item" -> ("p1" -> "v1") ~
      ("nested" -> ("value", ("p1" -> "v1") ~ ("p2" -> ("p3", "v3") ~ ("p4", "v4"))))
    val parsed = JsonFormatParser.parseItem(root, context, nested, time)
    val expected = Full(List(
      new KvinTuple(URIs.createURI("http://example.root/item"),
        URIs.createURI("http://example.root/p1"), Kvin.DEFAULT_CONTEXT, time, "v1"),
      new KvinTuple(URIs.createURI("http://example.root/item"),
        URIs.createURI("http://example.root/nested"), Kvin.DEFAULT_CONTEXT, time,
          new Record(URIs.createURI("http://example.root/p1"), "v1").append(
            new Record(URIs.createURI("http://example.root/p2"),
              new Record(URIs.createURI("http://example.root/p3"), "v3").append(
                new Record(URIs.createURI("http://example.root/p4"), "v4")
              )
            )
          )
      )
    ))
    Assert.assertEquals(expected, parsed)
  }

  @Test
  def shouldFail(): Unit = {
    val context = Kvin.DEFAULT_CONTEXT
    val time = System.currentTimeMillis
    val root = URIs.createURI("http://example.root")
    val itemError: JValue =
      ("some item" -> ("http://example.org/property" -> "val"))
    var result = JsonFormatParser.parseItem(root, context, itemError, time)
    assert(result.isInstanceOf[Failure])
    result match {
      case Failure(msg, _, _) =>
        assert(msg.contains("Invalid URI: some item"))
      case _ => fail("Expected Invalid URI: some item, got " + result)
    }

    val propertyError = ("item" -> ("http://example.org/my property" -> "val"))
    result = JsonFormatParser.parseItem(root, context, propertyError, time)
    assert(result.isInstanceOf[Failure])
    result match {
      case Failure(msg, _, _) =>
        assert(msg.contains("Invalid URI: http://example.org/my property"))
      case _ => fail("Expected Invalid URI: http://example.org/my property, got " + result)
    }
  }
}