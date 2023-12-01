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
 * @param <T>
 *            A sub-class of {@link KvinTuple}
 */
public abstract class AggregatingIterator<T extends KvinTuple> extends NiceIterator<T> {
	static final Logger log = LoggerFactory.getLogger(AggregatingIterator.class);

	final Iterator<T> base;
	final long interval;
	final String op;
	final long limit;

	T next;
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
		boolean hasNext = false;
		if (limit == 0 || count < limit) {
			hasNext = base.hasNext();
		}
		if (!hasNext) {
			close();
		}
		return hasNext;
	}

	@Override
	public T next() {
		// keeps elements of current active interval
		List<T> inInterval = new ArrayList<>();
		if (next == null) {
			next = base.next();
		}
		inInterval.add(next);

		long intervalStart = next.time - (next.time % interval);
		next = null;
		while (base.hasNext()) {
			T entry = base.next();
			long entryIntervalStart = entry.time - (entry.time % interval);
			if (entryIntervalStart != intervalStart) {
				next = entry;
				// starts new interval
				break;
			} else {
				inInterval.add(entry);
			}
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
