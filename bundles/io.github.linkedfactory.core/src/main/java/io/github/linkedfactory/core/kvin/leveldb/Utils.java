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
package io.github.linkedfactory.core.kvin.leveldb;

import io.github.linkedfactory.core.kvin.KvinTuple;

import java.nio.ByteBuffer;

/**
 * Helper functions for encoding keys.
 */
public final class Utils {
	private Utils() {
	}

	public static long invTime(long time) {
		return KvinTuple.TIME_MAX_VALUE - time;
	}

	public static int invSeq(int seq) {
		return KvinTuple.SEQ_MAX_VALUE - seq;
	}

	private static long createInt6(byte b5, byte b4, byte b3, byte b2, byte b1, byte b0) {
		return ((long) (b5 & 0xff) << 40) | ((long) (b4 & 0xff) << 32) | ((long) (b3 & 0xff) << 24) |
				((long) (b2 & 0xff) << 16) | ((long) (b1 & 0xff) << 8) | ((long) b0 & 0xff);
	}

	/**
	 * Reads a 6-byte integer at the buffer's position and advances the position.
	 */
	public static long getInt6(ByteBuffer bb) {
		int pos = bb.position();
		long result = getInt6(bb, pos);
		bb.position(pos + 6);
		return result;
	}

	/**
	 * Reads a 6-byte integer at the given index.
	 */
	public static long getInt6(ByteBuffer bb, int bi) {
		return createInt6(bb.get(bi), bb.get(bi + 1), bb.get(bi + 2), bb.get(bi + 3), bb.get(bi + 4), bb.get(bi + 5));
	}

	/**
	 * Writes a 6-byte integer at the buffer's position and advances the position.
	 */
	public static ByteBuffer putInt6(ByteBuffer bb, long v) {
		int pos = bb.position();
		putInt6(bb, pos, v);
		bb.position(pos + 6);
		return bb;
	}

	/**
	 * Writes a 6-byte integer at the given index.
	 */
	public static ByteBuffer putInt6(ByteBuffer bb, int bi, long v) {
		bb.put(bi, (byte) (v >> 40));
		bb.put(bi + 1, (byte) (v >> 32));
		bb.put(bi + 2, (byte) (v >> 24));
		bb.put(bi + 3, (byte) (v >> 16));
		bb.put(bi + 4, (byte) (v >> 8));
		bb.put(bi + 5, (byte) v);
		return bb;
	}

	/**
	 * Writes an unsigned short at the buffer's position and advances the position.
	 */
	public static ByteBuffer putShortUnsigned(ByteBuffer bb, int s) {
		int pos = bb.position();
		putShortUnsigned(bb, pos, s);
		bb.position(pos + 2);
		return bb;
	}

	/**
	 * Writes an unsigned short at the given index.
	 */
	public static ByteBuffer putShortUnsigned(ByteBuffer bb, int bi, int s) {
		bb.putShort(bi, (short) (s & 0xffff));
		return bb;
	}

	/**
	 * Reads an unsigned short at the buffer's position and advances the position.
	 */
	public static int getShortUnsigned(ByteBuffer bb) {
		int pos = bb.position();
		int result = getShortUnsigned(bb, pos);
		bb.position(pos + 2);
		return result;
	}

	/**
	 * Reads an unsigned short at the given index.
	 */
	public static int getShortUnsigned(ByteBuffer bb, int bi) {
		return bb.getShort(bi) & 0xffff;
	}
}
