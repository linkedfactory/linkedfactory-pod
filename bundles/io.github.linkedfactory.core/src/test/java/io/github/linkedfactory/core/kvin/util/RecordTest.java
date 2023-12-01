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
package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.Record;
import net.enilink.komma.core.URIs;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RecordTest {

	<T> Stream<T> toStream(Iterator<T> it) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
	}

	@Test
	public void testSimple() {
		Record r = new Record(URIs.createURI("p1"), "v1");
		Assert.assertEquals(1, r.size());
		Record r2 = new Record(URIs.createURI("p2"), "v2");
		Record combined = r.append(r2);
		Assert.assertEquals(2, combined.size());
	}

	@Test
	public void testNested() {
		Record r = new Record(URIs.createURI("p1"), "v1");
		Assert.assertEquals(1, r.size());
		Assert.assertEquals(1, toStream(r.iterator()).count());
		r = r.append(new Record(URIs.createURI("p2"), "v2"));
		Assert.assertEquals(2, r.size());
		Assert.assertEquals(2, toStream(r.iterator()).count());
		Record nested = new Record(URIs.createURI("p2"), "v2");
		nested = nested.append(new Record(URIs.createURI("nested"), r));
		Assert.assertEquals(2, nested.size());
		Assert.assertEquals(2, toStream(r.iterator()).count());
	}

	@Test
	public void testRemove() {
		Record r = new Record(URIs.createURI("p1"), "v1");
		r = r.append(new Record(URIs.createURI("p2"), "v2"));
		Assert.assertEquals(2, r.size());
		Assert.assertEquals(1, r.removeFirst(URIs.createURI("p1")).size());
		Assert.assertEquals(2, r.removeFirst(URIs.createURI("unknown")).size());
		Assert.assertEquals(2, r.removeFirst(URIs.createURI("unkown"), "v1").size());
		Assert.assertEquals(1, r.removeFirst(URIs.createURI("p1"), "v1").size());
		Assert.assertEquals(2, r.removeFirst(URIs.createURI("p1"), "unknown").size());
	}
}