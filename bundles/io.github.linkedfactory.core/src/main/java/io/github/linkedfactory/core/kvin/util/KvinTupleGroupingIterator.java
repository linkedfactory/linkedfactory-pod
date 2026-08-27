/*
 * Copyright (c) 2024 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reorders each bounded window of tuples into contiguous series. Series are
 * emitted in the order in which their identities first occur in the window;
 * tuples within a series retain their source order.
 */
public final class KvinTupleGroupingIterator extends NiceIterator<KvinTuple> {
	private final IExtendedIterator<KvinTuple> source;
	private final int windowSize;
	private final ArrayDeque<KvinTuple> pending = new ArrayDeque<>();
	private boolean finished;
	private boolean sourceClosed;

	public KvinTupleGroupingIterator(IExtendedIterator<KvinTuple> source, int windowSize) {
		if (source == null) {
			throw new NullPointerException("source");
		}
		if (windowSize <= 0) {
			throw new IllegalArgumentException("windowSize must be positive");
		}
		this.source = source;
		this.windowSize = windowSize;
	}

	@Override
	public boolean hasNext() {
		if (!pending.isEmpty()) {
			return true;
		}
		if (finished) {
			return false;
		}

		Map<Series, List<KvinTuple>> groups = new LinkedHashMap<>();
		try {
			int count = 0;
			boolean sourceExhausted = false;
			while (count < windowSize) {
				if (!source.hasNext()) {
					sourceExhausted = true;
					break;
				}
				KvinTuple tuple = source.next();
				groups.computeIfAbsent(new Series(tuple), key -> new ArrayList<>()).add(tuple);
				count++;
			}
			for (List<KvinTuple> group : groups.values()) {
				pending.addAll(group);
			}
			if (sourceExhausted) {
				closeSource();
			}
			if (count == 0) {
				finished = true;
			}
			return !pending.isEmpty();
		} catch (RuntimeException error) {
			finished = true;
			closeSourceSuppressing(error);
			throw error;
		} catch (Error error) {
			finished = true;
			closeSourceSuppressing(error);
			throw error;
		}
	}

	@Override
	public KvinTuple next() {
		if (!hasNext()) {
			throw new java.util.NoSuchElementException();
		}
		return pending.removeFirst();
	}

	@Override
	public void close() {
		finished = true;
		pending.clear();
		closeSource();
	}

	private void closeSource() {
		if (!sourceClosed) {
			sourceClosed = true;
			source.close();
		}
	}

	private void closeSourceSuppressing(Throwable original) {
		try {
			closeSource();
		} catch (Throwable closeError) {
			original.addSuppressed(closeError);
		}
	}

	private static final class Series {
		private final Object item;
		private final Object property;
		private final Object context;

		private Series(KvinTuple tuple) {
			item = tuple.item;
			property = tuple.property;
			context = tuple.context;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof Series)) {
				return false;
			}
			Series that = (Series) other;
			return Objects.equals(item, that.item) && Objects.equals(property, that.property)
					&& Objects.equals(context, that.context);
		}

		@Override
		public int hashCode() {
			return Objects.hash(item, property, context);
		}
	}
}
