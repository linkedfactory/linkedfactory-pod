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
package io.github.linkedfactory.core.kvin.records;

import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

/**
 * A key-value store for time series data based on {@link KvinRecord}.
 */
public interface KvinRecordStore extends Closeable {
	/**
	 * The default context ID that is used for <code>null</code> values.
	 */
	Long DEFAULT_CONTEXT_VALUE = 0L;

	/**
	 * The default context ID that is used for <code>null</code> values.
	 */
	Long DEFAULT_CONTEXT = DEFAULT_CONTEXT_VALUE;

	List<Long> NULL_PROPERTY = Collections.singletonList(-1L);

	/**
	 * Stores records.
	 *
	 * @param records The records that should be stored.
	 */
	void put(KvinRecord... records);

	/**
	 * Stores multiple values in a batch.
	 *
	 * @param records Iterable of records.
	 */
	void put(Iterable<KvinRecord> records);

	/**
	 * Fetches the last values of a given item and property.
	 *
	 * @param itemId     The item ID.
	 * @param propertyId The property ID.
	 * @param contextId  The context ID.
	 * @param limit      Maximum number of elements that should be fetched.
	 * @return A list of record values.
	 */
	IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long limit);

	/**
	 * Fetches the values of a given item and property within the time interval
	 * [begin, end].
	 *
	 * @param itemId     The item ID.
	 * @param propertyId The property ID.
	 * @param contextId  The context ID.
	 * @param end        The end of the time interval.
	 * @param begin      The beginning of the time interval.
	 * @param limit      Maximum number of elements that should be fetched.
	 * @param interval   Minimum distance (in milliseconds) between two data points
	 *                   starting from given end or from the timestamp of the most
	 *                   recent value.
	 * @param op         Operator that is used to aggregate the values within the given
	 *                   interval.
	 * @return A list of record values.
	 */
	IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long end, long begin, long limit,
	                                    long interval, String op);

	/**
	 * Fetches the values of given items and properties within the time interval
	 * [begin, end].
	 *
	 * @param itemIds     The item IDs.
	 * @param propertyIds The property IDs.
	 * @param contextId   The context ID.
	 * @param end         The end of the time interval.
	 * @param begin       The beginning of the time interval.
	 * @param limit       Maximum number of elements that should be fetched.
	 * @param interval    Minimum distance (in milliseconds) between two data points
	 *                    starting from given end or from the timestamp of the most
	 *                    recent value.
	 * @param op          Operator that is used to aggregate the values within the given
	 *                    interval.
	 * @return A list of record values.
	 */
	default IExtendedIterator<KvinRecord> fetch(List<Long> itemIds, List<Long> propertyIds, Long contextId, long end, long begin,
	                                            long limit, long interval, String op) {
		IExtendedIterator<KvinRecord> it = NiceIterator.emptyIterator();
		if (propertyIds.isEmpty()) {
			propertyIds = NULL_PROPERTY;
		}
		for (long itemId : itemIds) {
			for (Long propertyId : propertyIds) {
				// use lazy initialization for further iterators
				it = it.andThen(new NiceIterator<>() {
					IExtendedIterator<KvinRecord> base;

					@Override
					public boolean hasNext() {
						if (base == null) {
							base = fetch(itemId, propertyId, contextId, end, begin, limit, interval, op);
						}
						return base.hasNext();
					}

					@Override
					public KvinRecord next() {
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
	 * @param itemId     The item ID.
	 * @param propertyId The property ID.
	 * @param contextId  The context ID.
	 * @param end        The end of the time interval.
	 * @param begin      The beginning of the time interval.
	 * @return Number of deleted records.
	 */
	long delete(long itemId, Long propertyId, Long contextId, long end, long begin);

	/**
	 * Deletes the given item and all of its associated values from the store.
	 *
	 * @param itemId    The item ID.
	 * @param contextId The context ID.
	 * @return <code>true</code> if item exists in the store else <code>false</code>.
	 */
	boolean delete(long itemId, Long contextId);

	/**
	 * Returns all known properties of a given item.
	 *
	 * @param itemId    The item ID.
	 * @param contextId The context ID.
	 * @return A list with properties of the given item.
	 */
	IExtendedIterator<Long> properties(long itemId, Long contextId);

	/**
	 * Closes the store and frees resources.
	 */
	@Override
	void close();
}
