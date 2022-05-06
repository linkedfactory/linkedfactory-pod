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
package io.github.linkedfactory.service.util

import net.enilink.komma.core.URI
import net.liftweb.common.Box
import net.liftweb.common.Failure
import net.liftweb.http.S

object SnippetHelpers {
  import scala.language.implicitConversions
  
  implicit def uriToStr(uri: URI) = uri.toString
  def paramNotEmpty(name: String, msg: String) = S.param(name).map(_.trim).filterMsg(name + ":" + msg)(!_.isEmpty)

  def doAlert[T](box: Box[T]) = box match {
    case f @ Failure(msg, _, _) =>
      msg.split(":", 2) match {
        case Array(name, alert) => S.error("alert-" + name, alert)
        case _ => S.error(msg)
      }; f
    case n => n
  }

  def failure(name: String, msg: String) = Failure(name + ":" + msg)
}