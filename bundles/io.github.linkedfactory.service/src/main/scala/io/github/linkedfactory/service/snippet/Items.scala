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

import io.github.linkedfactory.core.kvin.Kvin
import io.github.linkedfactory.service.Data
import net.enilink.komma.core.IEntity
import net.enilink.platform.lift.util.{AjaxHelpers, CurrentContext, Globals}
import net.liftweb.common.{Empty, Full}
import net.liftweb.http.js.JsCmds._
import net.liftweb.util.ClearNodes
import net.liftweb.util.Helpers._

import scala.xml.NodeSeq

class Items {
  /**
   * Delete the node from the database.
   */
  def delete: NodeSeq => NodeSeq = CurrentContext.value.map { c =>
    AjaxHelpers.onEvents("onclick" :: Nil, _ => {
      val item = c.subject.asInstanceOf[IEntity]
      val result = if (item.getURI != null && Data.kvin.isDefined) {
        if (Data.kvin.get.delete(item.getURI, Globals.contextModel.vend.map(_.getURI).openOr(Kvin.DEFAULT_CONTEXT))) {
          item.getEntityManager.removeRecursive(item, true)
          Full(Alert("Test"))
        }
        Empty
      } else Empty
      result openOr Noop
    }) andThen "a [href]" #> "javascript://"
  } openOr ClearNodes
      
//      val t = project.getEntityManager.getTransaction
//      val result =
//        try {
//          for {
//            m <- Globals.contextModel.vend
//            modelSet = m.getModelSet
//            projectModel <- Option(modelSet.getModel(project.getURI, false))
//          } {
//            projectModel.getManager.clear
//            modelSet.getModels.remove(projectModel)
//            modelSet.getMetaDataManager.remove(projectModel)
//          }
//          t.begin
//          project.getEntityManager.removeRecursive(project, true)
//          t.commit
//          Full(Run(s"""$$('[about="${project.getURI}"]').remove()"""))
//        } catch {
//          case _: Throwable => if (t.isActive) t.rollback; Empty
//        }
//      result openOr Noop
//    }) andThen "a [href]" #> "javascript://"
//  } openOr ClearNodes
}
