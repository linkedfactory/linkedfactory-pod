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
package io.github.linkedfactory.kvin;

import java.util.Formatter;
import java.util.Objects;

import net.enilink.komma.core.URI;

/**
 * Represents a time-stamped row within the value store.
 */
public class KvinTuple {
	public static final long TIME_MAX_VALUE = 1L << (6 * 8) - 1;

	public static final int SEQ_MAX_VALUE = 1 << (2 * 8) - 1;

	public final URI item;
	public final URI property;
	public final long time;
	public final int seqNr;
	public final Object value;
	public final URI context;

	/**
	 * Creates a KVIN tuple.
	 *
	 * @param item     The item URI.
	 * @param property The property URI.
	 * @param context  The context URI.
	 * @param time     The associated time.
	 * @param value    The value at the given time.
	 */
	public KvinTuple(URI item, URI property, URI context, long time, Object value) {
		this(item, property, context, time, 0, value);
	}

	/**
	 * Creates a KVIN tuple.
	 *
	 * @param item       The item URI.
	 * @param property   The property URI.
	 * @param context    The context URI.
	 * @param time       The associated time.
	 * @param seqNr The sequence number.
	 * @param value      The value at the given time.
	 */
	public KvinTuple(URI item, URI property, URI context, long time, int seqNr, Object value) {
		if (time > TIME_MAX_VALUE) {
			throw new IllegalArgumentException(
					"Maximum range of time attribute exceeded: " + time + " > " + TIME_MAX_VALUE);
		}

		this.item = item;
		this.property = property;
		this.time = time;
		this.seqNr = seqNr;
		this.value = value;
		this.context = context;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof KvinTuple)) return false;
		KvinTuple kvinTuple = (KvinTuple) o;
		return time == kvinTuple.time && seqNr == kvinTuple.seqNr && item.equals(kvinTuple.item) &&
				property.equals(kvinTuple.property) && value.equals(kvinTuple.value) &&
				context.equals(kvinTuple.context);
	}

	@Override
	public int hashCode() {
		return Objects.hash(item, property, time, seqNr, value, context);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getClass().getSimpleName());
		Formatter f = new Formatter(sb);
		f.format("(item=%s, property=%s, context=%s, time=%s, seqNr=%s, value=%s", item, property, context, time,
				seqNr, value);
		f.close();
		return sb.toString();
	}
}
