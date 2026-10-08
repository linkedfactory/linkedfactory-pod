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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import io.github.linkedfactory.core.kvin.Kvin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.util.ValueUtils;
import net.enilink.komma.core.URI;
import io.github.linkedfactory.core.kvin.KvinTuple;

/**
 * An iterator for KVIN tuples supporting a set of aggregation operators (min,
 * max, sum, avg, first) using constant-space accumulation within a time range. This is
 * actually a helper class for {@link Kvin} compatible stores to provide
 * pre-aggregated values.
 *
 * @param <T> A sub-class of {@link KvinTuple}
 */
public abstract class AggregatingIterator<T extends KvinTuple> extends NiceIterator<T> {
	static final Logger log = LoggerFactory.getLogger(AggregatingIterator.class);

	final Iterator<T> base;
	final long interval;
	final String op;
	final long limit;
	private final boolean customAggregation;

	T next, baseNext;
	private T seriesFirst;
	int seqNr = 1;
	long count = 0;

	public AggregatingIterator(Iterator<T> base, long interval, String op, long limit) {
		this.base = base;
		this.interval = interval;
		this.op = op;
		this.limit = limit;
		Class<?> type = getClass();
		boolean customAggregation = false;
		while (type != AggregatingIterator.class) {
			try {
				type.getDeclaredMethod("aggregate", List.class, String.class);
				customAggregation = true;
				break;
			} catch (NoSuchMethodException e) {
				type = type.getSuperclass();
			}
		}
		this.customAggregation = customAggregation;
	}

	protected abstract T createElement(URI item, URI property, URI context, long time, int seqNr, Object value);

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}
		if (baseNext != null || base.hasNext()) {
			next = computeNext();
		}
		return next != null;
	}

	@Override
	public T next() {
		if (hasNext()) {
			T value = next;
			next = null;
			return value;
		}
		throw new NoSuchElementException();
	}

	protected T computeNext() {
		T first;
		while (true) {
			if (baseNext != null) {
				first = baseNext;
				baseNext = null;
			} else if (base.hasNext()) {
				first = base.next();
			} else {
				return null;
			}
			if (seriesFirst == null || !sameSeries(first, seriesFirst)) {
				seriesFirst = first;
				count = 0;
			}
			if (limit <= 0 || count < limit) {
				break;
			}
		}
		long intervalStart = intervalStart(first);
		Accumulator accumulator = new Accumulator(first.value, op);
		// Preserve the list-based extension hook only for subclasses that override it.
		List<T> elements = customAggregation ? new ArrayList<>() : null;
		if (elements != null) {
			elements.add(first);
		}
		boolean invalidNumber = false;
		while (base.hasNext()) {
			T entry = base.next();
			if (!sameSeries(first, entry) || intervalStart(entry) != intervalStart) {
				baseNext = entry;
				break;
			}
			if (elements != null) {
				elements.add(entry);
			} else if (!invalidNumber) {
				try {
					accumulator.add(entry.value);
				} catch (NumberFormatException nfe) {
					// Drain the rest of this interval, just as the buffered implementation did.
					invalidNumber = true;
				}
			}
		}
		count++;
		Object value;
		try {
			value = elements != null ? aggregate(elements, op) : invalidNumber ? 0 : accumulator.value();
		} catch (NumberFormatException nfe) {
			invalidNumber = true;
			value = 0;
		}
		if (invalidNumber) {
			log.error("Invalid number format for item {} and property {} in interval [{}, {}]", first.item,
					first.property, intervalStart, intervalStart + interval);
		}
		return createElement(first.item, first.property, first.context, intervalStart, seqNr++, value);
	}

	private boolean sameSeries(T left, T right) {
		return (left.item == right.item || left.item.equals(right.item)) &&
				(left.property == right.property || left.property.equals(right.property));
	}

	private long intervalStart(T tuple) {
		return interval == 0 ? 0 : tuple.time - (tuple.time % interval);
	}

	@Override
	public void close() {
		close(base);
	}

	/**
	 * Applies the given operator to the list of elements.
	 * Subclasses overriding this hook receive the complete interval; the default
	 * iterator path uses streaming accumulation instead of building a list.
	 */
	protected Object aggregate(List<T> elements, String op) {
		Iterator<T> it = elements.iterator();
		Accumulator accumulator = new Accumulator(it.next().value, op);
		while (it.hasNext()) {
			accumulator.add(it.next().value);
		}
		return accumulator.value();
	}

	private static final class Accumulator {
		private final ValueUtils utils = ValueUtils.getInstance();
		private final String op;
		private Object value;
		private long count = 1;

		private Accumulator(Object value, String op) {
			this.value = value;
			this.op = op;
		}

		private void add(Object current) {
			switch (op) {
				case "min":
					if (utils.compareWithConversion(value, current) > 0) {
						value = current;
					}
					break;
				case "max":
					if (utils.compareWithConversion(value, current) < 0) {
						value = current;
					}
					break;
				case "avg":
				case "sum":
					value = utils.add(value, current);
					break;
			}
			count++;
		}

		private Object value() {
			return "avg".equals(op) ? utils.divide(value, count) : value;
		}
	}
}
