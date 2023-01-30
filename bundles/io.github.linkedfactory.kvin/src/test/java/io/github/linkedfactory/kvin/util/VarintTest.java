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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class VarintTest {

	long[] values = new long[] {
			240, 2287, 67823, 16777215, 4294967295L, 1099511627775L, 281474976710655L, 72057594037927935L,
			72057594037927935L + 1
	};

	@Test
	public void testVarint() {
		ByteBuffer bb = ByteBuffer.allocate(9);
		for (int i = 0; i < values.length; i++) {
			bb.clear();
			Varint.writeUnsigned(bb, values[i]);
			bb.flip();
			assertEquals("Encoding should use " + (i + 1) + " bytes", i + 1, bb.remaining());
			assertEquals("Encoded and decoded value should be equal", values[i], Varint.readUnsigned(bb));
		}
	}

	@Test
	public void testVarintList() {
		ByteBuffer bb = ByteBuffer.allocate(2 + 4 * Long.BYTES);
		for (int i = 0; i < values.length - 4; i++) {
			long[] expected = new long[4];
			System.arraycopy(values, 0, expected, 0, 4);
			bb.clear();
			Varint.writeListUnsigned(bb, expected);
			bb.flip();
			long[] actual = new long[4];
			Varint.readListUnsigned(bb, actual);
			assertArrayEquals("Encoded and decoded value should be equal", expected, actual);
		}
	}
}