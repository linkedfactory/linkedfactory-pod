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

import net.enilink.komma.core.IEntityManager

object ResourceHelpers {

  def withTransaction[E <: IEntityManager, R](manager: E)(block: E => R): R = {
    val transaction = manager.getTransaction
    if (transaction.isActive) block(manager) else {
      transaction.begin
      try {
        val result = block(manager)
        transaction.commit
        result
      } catch {
        case ex: Throwable if transaction.isActive => {
          transaction.rollback
          throw new RuntimeException(ex)
        }
      }
    }
  }

  def doWith[C <: AutoCloseable, R](closeable: C)(block: C => R) {
    try {
      block(closeable)
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    } finally {
      closeable.close
    }
  }

}