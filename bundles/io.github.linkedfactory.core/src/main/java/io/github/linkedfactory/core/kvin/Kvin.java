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
package io.github.linkedfactory.core.kvin;

import java.io.Closeable;
import java.util.List;

import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

/**
 * A key-value store for time series data.
 */
public interface Kvin extends Closeable {
	/**
	 * The default context that is used for <code>null</code> values.
	 */
	String DEFAULT_CONTEXT_VALUE = "kvin:nil";

	/**
	 * The default context that is used for <code>null</code> values.
	 */
	URI DEFAULT_CONTEXT = URIs.createURI(DEFAULT_CONTEXT_VALUE);

	/**
	 * Add a listener to be notified of changes.
	 */
	boolean addListener(KvinListener listener);

	/**
	 * Remove a listener.
	 */
	boolean removeListener(KvinListener listener);

	/**
	 * Stores tuples.
	 *
	 * @param tuples The tuples that should be stored.
	 */
	void put(KvinTuple... tuples);

	/**
	 * Stores multiple values in a batch
	 *
	 * @param tuples Iterable of KVIN tuples.
	 */
	void put(Iterable<KvinTuple> tuples);

	/**
	 * Fetches the last values of a given item and property.
	 *
	 * @param item     The item URI.
	 * @param property The property URI.
	 * @param context  The context URI.
	 * @param limit    Maximum number of elements that should be fetched.
	 * @return A list of pairs of unique value URIs and associated values.
	 */
	IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit);

	/**
	 * Fetches the values of a given item and property within the time interval
	 * [begin, end].
	 *
	 * @param item     The item URI.
	 * @param property The property URI.
	 * @param context  The context URI.
	 * @param end      The end of the time interval.
	 * @param begin    The beginning of the time interval.
	 * @param limit    Maximum number of elements that should be fetched.
	 * @param interval Minimum distance (in milliseconds) between two data points
	 *                 starting from given end or from the timestamp of the most
	 *                 recent value.
	 * @param op       Operator that is used to aggregate the values within the given
	 *                 interval.
	 * @return A list of pairs of unique value URIs and associated values.
	 */
	IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit,
	                                   long interval, String op);

	/**
	 * Fetches the values of a given items and properties within the time interval
	 * [begin, end].
	 *
	 * @param items      The item URIs.
	 * @param properties The property URIs.
	 * @param context    The context URI.
	 * @param end        The end of the time interval.
	 * @param begin      The beginning of the time interval.
	 * @param limit      Maximum number of elements that should be fetched.
	 * @param interval   Minimum distance (in milliseconds) between two data points
	 *                   starting from given end or from the timestamp of the most
	 *                   recent value.
	 * @param op         Operator that is used to aggregate the values within the given
	 *                   interval.
	 * @return A list of pairs of unique value URIs and associated values.
	 */
	default IExtendedIterator<KvinTuple> fetch(List<URI> items, List<URI> properties, URI context, long end, long begin,
	                                           long limit, long interval, String op) {
		IExtendedIterator<KvinTuple> it = NiceIterator.emptyIterator();
		for (URI item : items) {
			for (URI property : properties) {
				// use lazy initialization for further iterators
				it = it.andThen(new NiceIterator<>() {
					IExtendedIterator<KvinTuple> base;

					@Override
					public boolean hasNext() {
						if (base == null) {
							base = fetch(item, property, context, end, begin, limit, interval, op);
						}
						return base.hasNext();
					}

					@Override
					public KvinTuple next() {
						ensureHasNext();
						return base.next();
					}

					@Override
					public void close() {
						base.close();
					}
				});
			}
		}
		return it;
	}

	/**
	 * Deletes the values of a given item and property within the time interval
	 * [begin, end].
	 *
	 * @param item     The item URI.
	 * @param property The property URI.
	 * @param context  The context URI.
	 * @param end      The end of the time interval.
	 * @param begin    The beginning of the time interval.
	 * @return Number of deleted records.
	 */
	long delete(URI item, URI property, URI context, long end, long begin);

	/**
	 * Deletes the given item and all of its associated values from the store.
	 *
	 * @param item    The item URI.
	 * @param context The context URI.
	 * @return <code>true</code> if item exists in the store else
	 * <code>false</code>.
	 */
	boolean delete(URI item, URI context);

	/**
	 * Returns all known sub-items of a given item.
	 *
	 * @param item    The item URI.
	 * @param context The context URI.
	 * @return A list with descendants of the given item.
	 */
	IExtendedIterator<URI> descendants(URI item, URI context);

	/**
	 * Returns all known sub-items of a given item.
	 *
	 * @param item    The item URI.
	 * @param context The context URI.
	 * @return A list with descendants of the given item.
	 */
	IExtendedIterator<URI> descendants(URI item, URI context, long limit);

	/**
	 * Returns all known properties of a given item.
	 *
	 * @param item    The item URI.
	 * @param context The context URI.
	 * @return A list with properties of the given item.
	 */
	IExtendedIterator<URI> properties(URI item, URI context);

	/**
	 * Closes the store and frees resources.
	 */
	@Override
	void close();
}
