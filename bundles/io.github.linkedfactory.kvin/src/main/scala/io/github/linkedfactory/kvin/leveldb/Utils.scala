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
package io.github.linkedfactory.kvin.leveldb

import io.github.linkedfactory.kvin.KvinTuple

import java.nio.ByteBuffer

object Utils {
  def invTime(time: Long) = KvinTuple.TIME_MAX_VALUE - time

  def invSeq(seq: Int) = KvinTuple.SEQ_MAX_VALUE - seq
  
  implicit class RichBuffer(bb: ByteBuffer) {
    private def createInt6(b5: Byte, b4: Byte, b3: Byte, b2: Byte, b1: Byte, b0: Byte) = {
      ((b5 & 0xff).toLong << 40) | ((b4 & 0xff).toLong << 32) | ((b3 & 0xff).toLong << 24) |
        ((b2 & 0xff).toLong << 16) | ((b1 & 0xff).toLong << 8) | (b0.toLong & 0xff)
    }

    def getInt6: Long = {
      val pos = bb.position()
      val result = getInt6(pos)
      bb.position(pos + 6)
      result
    }

    def getInt6(bi: Int): Long = createInt6(bb.get(bi), bb.get(bi + 1), bb.get(bi + 2), bb.get(bi + 3), bb.get(bi + 4), bb.get(bi + 5))

    def putInt6(v: Long): ByteBuffer = {
      val pos = bb.position()
      putInt6(pos, v)
      bb.position(pos + 6)
      bb
    }

    def putInt6(bi: Int, v: Long): ByteBuffer = {
      bb.put(bi, (v >> 40).toByte)
      bb.put(bi + 1, (v >> 32).toByte)
      bb.put(bi + 2, (v >> 24).toByte)
      bb.put(bi + 3, (v >> 16).toByte)
      bb.put(bi + 4, (v >> 8).toByte)
      bb.put(bi + 5, v.toByte)
      bb
    }

    def putShortUnsigned(s: Int): ByteBuffer = {
      val pos = bb.position()
      putShortUnsigned(bb.position(), s)
      bb.position(pos + 2)
      bb
    }

    def putShortUnsigned(bi: Int, s: Int): ByteBuffer = {
      bb.putShort(bi, (s & 0xffff).toShort)
      bb
    }

    def getShortUnsigned: Int = {
      val pos = bb.position()
      val result = getShortUnsigned(bb.position())
      bb.position(pos + 2)
      result
    }

    def getShortUnsigned(bi: Int): Int = bb.getShort(bi) & 0xffff
  }
}