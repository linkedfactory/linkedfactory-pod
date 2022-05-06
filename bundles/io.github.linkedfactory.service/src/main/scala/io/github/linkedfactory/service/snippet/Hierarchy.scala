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
package io.github.linkedfactory.service.snippet

import net.enilink.komma.core.URI
import net.liftweb.http.S
import net.liftweb.sitemap.Loc
import net.liftweb.util.Helpers._

import scala.xml.{NodeSeq, Text}

class Hierarchy(item: URI) {
  val objectPath = item.segments
  val base = item.trimSegments(item.segmentCount)

  def allButLast(n: NodeSeq): NodeSeq = if (objectPath.length <= 1) Text("") else {
    val breadcrumbs = for {
      // for generating the links, get the menu entry from the request path
      req <- S.request;
      path <- req.location;
      menuLoc = path.breadCrumbs.reverse(0)
    } yield {
      val list = objectPath.toList.zipWithIndex.map {
        case (entry, idx) =>
          (entry, menuLoc.asInstanceOf[Loc[URI]].calcHref(base.appendSegments(objectPath.take(idx + 1))))
      }

      ("li *" #> list.take(list.length - 1).map {
        entry => {
          "a *" #> entry._1 & "a [href]" #> entry._2
        }
      }) apply n
    }
    breadcrumbs openOr Text("")
  }

  def last = ("li *" #> objectPath.last)

}
