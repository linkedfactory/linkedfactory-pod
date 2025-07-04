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
 * max, sum, avg) that required all values within a given time range. This is
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

	T next, baseNext;
	int seqNr = 1;
	long count = 0;

	public AggregatingIterator(Iterator<T> base, long interval, String op, long limit) {
		this.base = base;
		this.interval = interval;
		this.op = op;
		this.limit = limit;
	}

	protected abstract T createElement(URI item, URI property, URI context, long time, int seqNr, Object value);

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}
		if (base.hasNext()) {
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
		// keeps elements of current active interval
		List<T> inInterval = new ArrayList<>();
		long intervalStart = -1;
		if (baseNext != null && (limit == 0 || count < limit)) {
			inInterval.add(baseNext);
			intervalStart = interval == 0 ? 0 : baseNext.time - (baseNext.time % interval);
		}
		T prev = baseNext;
		baseNext = null;
		while (base.hasNext()) {
			T entry = base.next();
			if (prev != null && (entry.item != prev.item && !entry.item.equals(prev.item) ||
					entry.property != prev.property && !entry.property.equals(prev.property))) {
				baseNext = entry;
				count = 0;

				if (!inInterval.isEmpty()) {
					// start new interval if item or property changes and current interval is not empty
					break;
				}
			}

			// skip values of same item and property if required
			if (limit > 0 && count >= limit) {
				continue;
			}

			long entryIntervalStart = interval == 0 ? 0 : entry.time - (entry.time % interval);
			if (intervalStart < 0) {
				intervalStart = entryIntervalStart;
			}

			if (entryIntervalStart != intervalStart) {
				baseNext = entry;
				// start new interval
				break;
			} else {
				inInterval.add(entry);
				prev = entry;
			}
		}

		// item and property has not changed
		if (limit > 0 && count >= limit) {
			return null;
		}

		// no values to aggregate
		if (inInterval.isEmpty()) {
			return null;
		}

		count++;
		Object value;
		T first = inInterval.get(0);
		try {
			value = aggregate(inInterval, op);
		} catch (NumberFormatException nfe) {
			log.error("Invalid number format for item {} and property {} in interval [{}, {}]", first.item,
					first.property, intervalStart, intervalStart + interval);
			value = 0;
		}
		return createElement(first.item, first.property, first.context, intervalStart, seqNr++, value);
	}

	@Override
	public void close() {
		close(base);
	}

	/**
	 * Applies the given operator to the list of elements.
	 */
	protected Object aggregate(List<T> elements, String op) {
		ValueUtils utils = ValueUtils.getInstance();
		Iterator<T> it = elements.iterator();
		Object value = it.next().value;
		switch (op) {
			case "first":
				// just use first value
				break;
			case "min":
				while (it.hasNext()) {
					Object current = it.next().value;
					if (utils.compareWithConversion(value, current) > 0) {
						value = current;
					}
				}
				break;
			case "max":
				while (it.hasNext()) {
					Object current = it.next().value;
					if (utils.compareWithConversion(value, current) < 0) {
						value = current;
					}
				}
				break;
			case "avg":
				long count = 1;
				while (it.hasNext()) {
					Object current = it.next().value;
					value = utils.add(value, current);
					count++;
				}
				value = utils.divide(value, count);
				break;
			case "sum":
				while (it.hasNext()) {
					Object current = it.next().value;
					value = utils.add(value, current);
				}
				break;
		}
		return value;
	}
}
