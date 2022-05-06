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

import io.github.linkedfactory.service.util.ItemDataParser
import net.enilink.komma.core.URIs
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import org.junit.{Assert, Test}


class ItemDataParserTest {

  @Test
  def test : Unit  = {

    val time = System.currentTimeMillis
    val root = URIs.createURI("http://example.root")
    val simpleJson : JValue =
      ("@context" -> ("pref" -> "http://test1.example/")) ~
        ("pref" -> ("pref:rest" -> "val")  ~ ("pref2:pref3" -> "val2"))
    val (resource, property,_, value) = ItemDataParser.parseItem(root, simpleJson, time).head.head
    Assert.assertEquals("http://test1.example/", resource.toString)
    Assert.assertEquals("http://test1.example/rest", property.toString)
    Assert.assertEquals("val", value)
    val  (resource2, property2,_, value2) = ItemDataParser.parseItem(root, simpleJson, time).head.tail.head
    Assert.assertEquals("http://test1.example/", resource2.toString)
    Assert.assertEquals("http://example.root/pref2/pref3", property2.toString)
    Assert.assertEquals("val2", value2)

    val withoutContext = ("pref" -> ("pref:rest" -> "val")  ~ ("pref2:pref3" -> "val2"))
    val (r,p,_,v) = ItemDataParser.parseItem(root,withoutContext,time).head.head
    Assert.assertEquals("http://example.root/pref", r.toString)
    Assert.assertEquals("http://example.root/pref/rest", p.toString)
    Assert.assertEquals("val", v)

    val withMultiContext = ("@context" -> ("pref" -> "http://test1.example/") ~ ("pref2" -> "http://pref2.example/")) ~
      ("@context" -> ("pref" -> "http://test2.example/"))  ~
      ("pref" -> ("pref:rest" -> "val")  ~ ("pref2:rest" -> "val2"))
    val(r2,p2,_,v2) = ItemDataParser.parseItem(root, withMultiContext, time).head.head
    Assert.assertEquals("http://test2.example/", r2.toString)
    Assert.assertEquals("http://test2.example/rest", p2.toString)
    Assert.assertEquals("val", v2)
    val(r3,p3,_,v3) = ItemDataParser.parseItem(root, withMultiContext, time).head.tail.head
    Assert.assertEquals("http://test2.example/", r3.toString)
    Assert.assertEquals("http://pref2.example/rest", p3.toString)
    Assert.assertEquals("val2", v3)

    val prefixInContext = ("@context" -> ("pref" -> "http://test1.example/") ~ ("pref2" -> "pref:pref1")) ~
      ("@context" -> ("pref" -> "http://test2.example/"))  ~
      ("pref" -> ("pref:rest" -> "val")  ~ ("pref2" -> "val2"))
    val (_, p4, _, _) = ItemDataParser.parseItem(root, prefixInContext, time).head.tail.head
    Assert.assertEquals(p4.toString, "http://test1.example/pref1")

    val multiPrefixes =  ("@context" -> ("pref" -> "http://test1.example/") ~ ("pref2" -> "pref:pref1")) ~
      ("pref:pref1/pref2" -> ("pref:rest" -> "val")  ~ ("pref2" -> "val2"))
    val (r4,_,_,_) = ItemDataParser.parseItem(root, multiPrefixes, time).head.head
    Assert.assertEquals(r4.toString, "http://test1.example/pref1/pref2")

  }

}
