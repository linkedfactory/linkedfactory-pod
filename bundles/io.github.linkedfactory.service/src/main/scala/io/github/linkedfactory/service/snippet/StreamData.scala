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

import scala.xml.NodeSeq
import scala.xml.NodeSeq.seqToNodeSeq
import io.github.linkedfactory.service.{ItemLoc, LiftModule}
import net.enilink.komma.core.URIs
import net.liftweb.common.Full
import net.liftweb.http.PageName
import net.liftweb.http.S
import net.liftweb.sitemap.SiteMap
import net.liftweb.util.Helpers.urlEncode

class StreamData {
  def render(in: NodeSeq): NodeSeq = {
    lazy val baseURI = S.request.map { r => URIs.createURI(r.hostAndPath) } openOr URIs.createURI("")
    lazy val menuEntry = in \@ "data-menu-entry" match { case name if (!name.isEmpty()) => name case _ => "" }
    lazy val menuLoc = SiteMap.findAndTestLoc(menuEntry) match {
      case Full(l: ItemLoc) => l
      case _ => LiftModule.VIEW_MENU
    }
    val itemsBox = (S.attr("items") or menuLoc.currentValue) map (_.toString)
    itemsBox map {
      items =>
        items.split("\\s+") map { item =>
          val uri = URIs.createURI(item)
          if (uri.isRelative) uri.resolve(baseURI) else uri
        } mkString " "
    } map {
      items =>
        val limit = S.attr("limit")
        val itemsEncoded = urlEncode(items)
        val comet = <div data-lift={
          "comet?type=StreamDataActor;name=" + (limit.map(_ + "$$") openOr "") + itemsEncoded +
            ";items=" + itemsEncoded +
            (limit.map(";limit=" + _) openOr "")
        }></div>
        // eager evaluation to clear the original items and limit attributes
        S.clearAttrs {
          S.session.map {
            s => s.processSurroundAndInclude(PageName.get, comet)
          } openOr Nil
        }
    } openOr Nil
  }
}
