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
package io.github.linkedfactory.core.kvin.cli

import io.github.linkedfactory.core.kvin.{Kvin, KvinTuple}
import net.enilink.komma.core.URIs

import scala.jdk.CollectionConverters._

class ItemsCmd extends CLICommand {
  def description = "Retrieve items under a given root URI."

  def name = "items"

  def run(kvin: Kvin, args: String*) {
    val root = if (args.length > 0) {
      URIs.createURI(args(0))
    } else URIs.createURI("")

    kvin.descendants(root).toList.asScala.foreach {
      uri => println(uri)
    }
  }

  def usage = name + " [root URI]";
}

class PropertiesCmd extends CLICommand {
  def description = "Retrieve properties of a given item."

  def name = "properties"

  def run(kvin: Kvin, args: String*) {
    if (args.length > 0) {
      val item = URIs.createURI(args(0))

      kvin.properties(item).toList.asScala.foreach {
        uri => println(uri)
      }
    } else println("usage: " + usage)
  }

  def usage = name + " item";
}

class FetchCmd extends CLICommand {
  def description = "Retrieve values of a given item and property."

  def name = "fetch"

  def run(kvin: Kvin, args: String*) {
    if (args.length > 2) {
      val item = URIs.createURI(args(0))
      val property = args(1).toLowerCase

      val FromTo = "([^-]*)-([^-]*)".r
      val fromTo = args(2) match {
        case FromTo(from, to) =>
          (if (from.isEmpty) 0 else from.toLong, if (to.isEmpty) KvinTuple.TIME_MAX_VALUE else to.toLong)
      }

      val limit = if (args.length > 3) args(3).toLong else 0L

      kvin.properties(item).toList.asScala.find { _.toString.toLowerCase.contains(property) } match {
        case Some(propertyUri) =>
          println(item + "\t" + propertyUri)

          kvin.fetch(item, propertyUri, null, fromTo._2, fromTo._1, limit, 0, null)
            .iterator.asScala.foreach {
            tuple => println(tuple.time + "\t" + tuple.value)
          }
        case None =>
          System.err.println("No matching property found.")
      }
    } else println("usage: " + usage)
  }

  def usage = name + " item property from-to [limit]";
}