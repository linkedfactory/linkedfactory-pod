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
import net.enilink.platform.lift.snippet.Rdfa
import net.enilink.platform.lift.snippet.QueryParams
import net.enilink.komma.core.URI
import net.liftweb.http.S
import net.liftweb.http.PageName
import net.enilink.platform.lift.util.Globals
import net.enilink.platform.lift.util.RdfContext
import net.enilink.platform.lift.util.CurrentContext

class GetObjectRdfa(objectURI: URI) {
  def render(in: NodeSeq): NodeSeq = CurrentContext.withSubject(objectURI) {
    S.session.toSeq.flatMap(_.processSurroundAndInclude(PageName.get, in))
  }
}