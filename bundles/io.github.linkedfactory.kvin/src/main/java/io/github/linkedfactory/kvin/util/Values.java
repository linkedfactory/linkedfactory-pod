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
package io.github.linkedfactory.kvin.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Helper methods to translate values to and from their binary
 * representation required for a typical key-value store.
 */
public class Values {
	/**
	 * Decode a byte array into a primitive Java value or a string.
	 */
	public static Object decode(byte[] data) throws IOException {
		return decode(data, 0, data.length);
	}

	/**
	 * Decode a byte array into a primitive Java value or a string.
	 */
	public static Object decode(byte[] data, int offset, int length) throws IOException {
		var buffer = ByteBuffer.wrap(data, offset, length).order(ByteOrder.BIG_ENDIAN);
		return decode(buffer);
	}

	/**
	 * Decode bytes in a buffer into a primitive Java value or a string.
	 */
	public static Object decode(ByteBuffer buffer) throws IOException {
		byte discriminator = buffer.get();
		switch (discriminator) {
			case 'B':
				return buffer.get();
			case 'C':
				return buffer.getChar();
			case 'D':
				return buffer.getDouble();
			case 'F':
				return buffer.getFloat();
			case 'i':
				return (int)(-Varint.readUnsigned(buffer));
			case 'j':
				return -Varint.readUnsigned(buffer);
			case 's':
				return (short)(-Varint.readUnsigned(buffer));
			case 'I':
				return (int) Varint.readUnsigned(buffer);
			case 'J':
				return Varint.readUnsigned(buffer);
			case 'S':
				return (short) Varint.readUnsigned(buffer);
			case 'Z':
				return buffer.get() != 0;
			// string
			case '"':
				var b = new byte[(int) Varint.readUnsigned(buffer)];
				buffer.get(b);
				return new String(b, StandardCharsets.UTF_8);
			default:
				throw new IOException("Invalid discriminator: " + ((char) discriminator));
		}
	}

	private static ByteBuffer buffer(char t, int bitLength) {
		return ByteBuffer.allocate(1 + bitLength / 8).order(ByteOrder.BIG_ENDIAN).put((byte) t);
	}

	private static ByteBuffer encodeLong(char t, long v) {
		var bb = buffer(t, Varint.calcLengthUnsigned(v) * 8);
		Varint.writeUnsigned(bb, v);
		return bb;
	}

	/**
	 * Encode a primitive Java value or a string into a byte array.
	 */
	public static byte[] encode(Object value) {
		if (value instanceof Byte) {
			return buffer('B', java.lang.Byte.SIZE).put((Byte) value).array();
		}
		if (value instanceof Character) {
			return buffer('C', java.lang.Character.SIZE).putChar((Character) value).array();
		}
		if (value instanceof Double) {
			return buffer('D', java.lang.Double.SIZE).putDouble((Double) value).array();
		}
		if (value instanceof Float) {
			return buffer('F', java.lang.Float.SIZE).putFloat((Float) value).array();
		}
		if (value instanceof Integer) {
			int i = (Integer) value;
			return i < 0 ? encodeLong('i', -i).array() : encodeLong('I', i).array();
		}
		if (value instanceof Long) {
			long j = (Long) value;
			return j < 0 ? encodeLong('j', -j).array() : encodeLong('J', j).array();
		}
		if (value instanceof Short) {
			short s = (Short) value;
			return s < 0 ? encodeLong('s', -s).array() : encodeLong('S', s).array();
		}
		if (value instanceof Boolean) {
			return buffer('Z', java.lang.Byte.SIZE).put((byte) ((Boolean) value ? 1 : 0)).array();
		}
		if (value instanceof String) {
			var b = ((String) value).getBytes(StandardCharsets.UTF_8);
			var length = Varint.calcLengthUnsigned(b.length) + b.length;
			var buffer = buffer('"', length * 8);
			Varint.writeUnsigned(buffer, b.length);
			return buffer.put(b).array();
		}
		if (value instanceof BigInteger) {
			// store big integers as longs (possible loss of range)
			var longValue = ((BigInteger)value).longValue();
			return longValue < 0 ? encodeLong('j', -longValue).array() : encodeLong('J', longValue).array();
		}
		if (value instanceof BigDecimal) {
			// store big decimals as doubles (possible loss of range/precision)
			return buffer('D', java.lang.Double.SIZE).putDouble(((BigDecimal)value).doubleValue()).array();
		}
		throw new IllegalArgumentException("Unsupported data type of value " + value);
	}

	public static int skip(ByteBuffer buffer) throws IOException {
		// TODO implement this operation efficiently
		var start = buffer.position();
		decode(buffer);
		return buffer.position() - start;
	}
}