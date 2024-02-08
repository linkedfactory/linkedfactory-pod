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

import io.github.linkedfactory.service.LiftModule
import net.enilink.komma.core.URIs
import net.liftweb.util.Helpers._

import scala.xml.NodeSeq

class Util {
  def href(in: NodeSeq): NodeSeq = {
    val parent = in \@ "data-parent"
    val childURI = URIs.createURI(in \@ "data-child")
    val xform = "^ [href]" #> LiftModule.VIEW_MENU.calcHref(childURI) & "^ *" #> {
      if (parent.nonEmpty) {
        val parentURI = URIs.createURI(parent) match {
          case uri if uri.lastSegment != "" => uri.appendSegment("")
          case uri => uri
        }
        childURI.deresolve(parentURI).toString
      } else childURI.toString
    }
    xform(in)
  }

  def versionInfo = {
    "^" #> LiftModule.versionInfo
  }
}
