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

import net.enilink.komma.core.URIs

import scala.xml.NodeSeq
import scala.xml.NodeSeq.seqToNodeSeq
import net.liftweb.http.S
import net.liftweb.http.LiftRules
import net.liftweb.util.Helpers.tryo

class Watcher {
  def render(ns: NodeSeq): NodeSeq = {
    // suppress X-Frame-Options header to allow embedding via iframe
    val origHeaders = LiftRules.supplementalHeaders.vend
    LiftRules.supplementalHeaders.request.set(() => {
      origHeaders.filter { case (key, _) => key != "X-Frame-Options" }
    })

    val items = (S.param("item") or S.param("items")) getOrElse ""
    val properties = S.param("property") or S.param("properties")

    val limit = S.param("limit")
    <script data-lift="head">
      {
        """
$(document).on("stream-init stream-update", function(evt, data) {
  (window.parent || window).postMessage('{ "' + evt.type + '" : ' + JSON.stringify(data) + '}', '*');
});"""
      }
    </script>
    <div data-lift={ "StreamData?items=" + items +
      (properties.map(";properties=" + _) openOr "") +
      (limit.map(";limit=" + _) openOr "") }></div>
  }
}