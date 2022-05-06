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

import net.enilink.platform.lift.util.{CurrentContext, TemplateHelpers}
import net.liftweb.http.SHtml
import net.liftweb.http.js.JsCmds._
import net.liftweb.util.Helpers._

import scala.xml.NodeSeq

class LoadFeatures {
  def render = {
    val ctx = CurrentContext.value

    def renderTemplate = CurrentContext.withValue(ctx) {
      TemplateHelpers.render("linkedfactory" :: "_features" :: Nil)
    }

    val id = nextFuncName

    ".features-toggle *" #> ((n: NodeSeq) => {
      <a href="javascript://" onclick={SHtml.onEvent { x =>
        renderTemplate map {
          case (ns, script) => SetHtml(id, ns) & (script map (Run(_)) openOr Noop) & Run(s"$$('#$id').trigger('show-features')") &
            Run(s"enilink.ui.enableEdit('#features *')")
        } openOr Noop
      }.cmd}>
        {n}
      </a>
    }) & ".features-content [id]" #> id
  }
}