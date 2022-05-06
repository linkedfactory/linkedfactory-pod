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
package io.github.linkedfactory.service.snippet.linkedfactory

import net.enilink.komma.core.{URI, URIs}
import net.enilink.platform.lift.util.{CurrentContext, TemplateHelpers}
import net.liftweb.http.js.JE._
import net.liftweb.http.js.{JsCmd, JsCmds}
import net.liftweb.http.{S, SHtml}
import net.liftweb.util.Helpers._

import scala.xml.{Elem, NodeSeq}

class LoadNavigation(itemUri: URI) {
  def createRefreshFunc: String = {
    val ajaxCallback = (funcId: String) => {
      val result = {
        val item = S.param("item").map(URIs.createURI(_)) openOr itemUri
        val renderResult = CurrentContext.withSubject(item) {
          TemplateHelpers.render("linkedfactory" :: "_navigation" :: Nil)
        }
        renderResult map {
          case (html, script) =>
            val xform = ".pagination" #> {
              "a" #> ((n: NodeSeq) => n map {
                // some 'magic' to correctly load content rendered via RDFa snippet with pagination
                case e: Elem =>
                  val href = (e \ "@href").toString
                  if (href.startsWith("javascript:")) e else {
                    val params = href.replaceAll("[^?]+\\?", "").replaceAll("&amp;", "&")
                    e % ("href", "javascript://") % ("onclick", callAjaxRefresh(funcId, params).toJsCmd)
                  }
                case other => other
              })
            }

            xform.apply(html)
        } openOr <div>Error: Template not found.</div>
      }
      JsCmds.SetHtml(funcId, result)
    }
    // create refresh function and return the corresponding id
    S.fmapFunc(S.contextFuncBuilder(ajaxCallback))(name => name)
  }

  def callAjaxRefresh(name: String, additionalParams: String): JsCmd = {
    SHtml.makeAjaxCall(Str(name + "=" + name + "&" + additionalParams)).cmd
  }

  def render: NodeSeq => NodeSeq = {
    val refreshFuncId = createRefreshFunc
    val itemParam = paramsToUrlParams(("item", itemUri.toString) :: Nil)
    // bind button for loading the children
    val xform = "button [onclick]" #> {
      callAjaxRefresh(refreshFuncId, itemParam)
    } &
      // set id for the target element of loaded children
      ".nav-children [id]" #> refreshFuncId

    xform andThen ".search-form" #> {
      "button [onclick]" #> {
        SHtml.makeAjaxCall(JsRaw(s"'$refreshFuncId=$refreshFuncId&$itemParam&q=' + $$('.search-form input').val()")).cmd
      }
    }
  }
}