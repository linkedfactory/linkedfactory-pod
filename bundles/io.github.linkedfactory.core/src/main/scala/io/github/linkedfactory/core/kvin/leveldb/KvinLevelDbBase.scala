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
package io.github.linkedfactory.core.kvin.leveldb

import io.github.linkedfactory.core.kvin.util.Varint
import net.enilink.commons.iterator.NiceIterator
import net.enilink.komma.core.URI
import org.iq80.leveldb.{DB, DBIterator}

import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService}
import scala.util.matching.Regex

/**
 * Common base functions for LevelDB based value stores.
 */
trait KvinLevelDbBase {
  val LONG_BYTES: Int = java.lang.Long.SIZE / 8
  val BYTE_ORDER: ByteOrder = ByteOrder.BIG_ENDIAN

  val waitingForTTL: util.Set[ByteBuffer] = Collections.newSetFromMap[ByteBuffer](new ConcurrentHashMap[ByteBuffer, java.lang.Boolean])
  val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor

  abstract class StoreIterator[T](base: DBIterator) extends NiceIterator[T] {
    var open = true
    var current: T = _

    override def hasNext: Boolean = {
      if (current != null) true
      else if (open && base.hasNext) {
        current = computeNext
        if (current != null) true else {
          close()
          false
        }
      } else {
        if (open) close()
        false
      }
    }

    override def next: T = if (hasNext) {
      val result = current
      current = null.asInstanceOf[T]
      result
    } else throw new NoSuchElementException

    def computeNext: T

    override def close(): Unit = {
      if (open) {
        try {
          base.close()
        } finally {
          open = false
        }
      }
    }
  }

  protected def putLong(db: DB, key: Array[Byte], value: Long): Unit = {
    val bb = ByteBuffer.wrap(new Array[Byte](LONG_BYTES)).order(BYTE_ORDER)
    bb.putLong(value)
    db.put(key, bb.array)
  }

  protected def readLong(db: DB, key: Array[Byte]): Long = {
    val longBytes = db.get(key)
    if (longBytes == null) -1L else {
      ByteBuffer.wrap(longBytes).order(BYTE_ORDER).getLong
    }
  }

  // TTL support

  val TTL: Regex = "(?:^|&)ttl=([0-9]+)(ns|us|ms|s|m|d)".r
  protected def ttl(uri: URI): Option[Long] = {
    val query = uri.query
    if (query == null) None else query match {
      case TTL(durationStr, unit) =>
        val d = durationStr.toLong
        Some(unit match {
          case "ns" => d / 1000000L
          case "us" => d / 1000L
          case "ms" => d
          case "s" => d * 1000L
          case "m" => d * 60000L
          case "d" => d * 1440000L
        })
      case _ => None
    }
  }

  protected def asyncRemoveByTtl(db: DB, prefix: Array[Byte], ttl: Long): Unit = {
    val key = ByteBuffer.wrap(prefix)
    if (waitingForTTL.add(key)) {
      executor.submit(new Runnable {
        def run(): Unit = {
          removeByTtl(db, prefix, ttl)
          waitingForTTL.remove(key)
        }
      })
    }
  }

  protected def removeByTtl(db: DB, prefix: Array[Byte], ttl: Long): Unit = {
    val now = System.currentTimeMillis
    val eldest = now - ttl
    val idTimePrefix = new Array[Byte](prefix.length + Varint.MAX_BYTES)
    val bb = ByteBuffer.wrap(idTimePrefix).order(ByteOrder.BIG_ENDIAN)
    bb.put(prefix)
    writeVarint(bb, eldest)
    val it = db.iterator
    try {
      var done = false
      it.seek(idTimePrefix)
      while (it.hasNext && !done) {
        val entry = it.next
        val key = entry.getKey
        if (key.startsWith(prefix)) {
          db.delete(key)
        } else done = true
      }
    } finally {
      it.close()
    }
  }

  def writeVarint(byteBuffer: ByteBuffer, value : Long) : Unit = {
    Varint.writeUnsignedInverted(byteBuffer, value)
  }

  def writeVarint(byteBuffer: ByteBuffer, pos : Int, value : Long) : Unit = {
    Varint.writeUnsignedInverted(byteBuffer, pos, value)
  }

  def readVarint(byteBuffer: ByteBuffer) : Long = {
    Varint.readUnsignedInverted(byteBuffer)
  }
}
