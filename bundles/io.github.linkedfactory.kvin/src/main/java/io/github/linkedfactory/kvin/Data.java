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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

/**
 * A linked list of (property, value) pairs where properties are {@link URI}s
 * and values are arbitrary objects. Duplicate properties are explicitly
 * permitted.
 * 
 * This is inspired by RDF and the scala.xml.MetaData implementation.
 */
public abstract class Data<T extends Data<T>> implements Iterable<T> {
	public static final URI PROPERTY_VALUE = URIs.createURI("lf:value");

	protected final URI property;
	protected final T next;
	protected final Object value;

	protected Data(URI property, Object value, T next) {
		this.property = property;
		this.value = value;
		this.next = next;
	}

	/**
	 * Appends a data list to this list and returns a new copy.
	 * 
	 * @param data
	 *            The data list that should be appended
	 * @return A new list with <code>data</code> at the end
	 */
	public T append(T data) {
		if (this.property == null) {
			return data;
		} else if (this.next == null) {
			return copy(data);
		}
		return copy(next.append(data));
	}

	/**
	 * Create a copy of this data element.
	 * 
	 * @param next
	 *            The new next element
	 * @return A copy of this data element with a new next element.
	 */
	public abstract T copy(T next);

	/**
	 * Find the first data element within this list for the given property.
	 * 
	 * @param property
	 *            The property
	 * @return The data element for the given property or an empty data element
	 *         if it was not found.
	 */
	@SuppressWarnings("unchecked")
	public T first(URI property) {
		if (property != null) {
			if (property.equals(this.property)) {
				return (T) this;
			}
			if (next != null) {
				return next.first(property);
			}
		}
		return NULL();
	}

	/**
	 * Returns the property of this data element.
	 * 
	 * @return The property
	 */
	public URI getProperty() {
		return property;
	}

	/**
	 * Returns the value of this data element.
	 * 
	 * @return The value
	 */
	public Object getValue() {
		return value;
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			@SuppressWarnings("unchecked")
			T next = property != null ? (T) Data.this : null;
			T current = null;

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public T next() {
				if (next == null) {
					throw new NoSuchElementException();
				}
				current = next;
				next = current.next;
				return current;
			}

			@Override
			public void remove() {
				if (current == null) {
					throw new IllegalStateException();
				}
				current.removeFirst(current.property);
			}
		};
	}

	/**
	 * Remove the first element with the given property.
	 * 
	 * @param property
	 *            The property whose element should be removed
	 * @return A copy of this data list where the first corresponding has been
	 *         removed.
	 */
	@SuppressWarnings("unchecked")
	public T removeFirst(URI property) {
		if (this.property == null || property == null) {
			return (T) this;
		}
		if (property.equals(this.property)) {
			return next;
		}
		T newNext = next == null ? null : next.removeFirst(property);
		return newNext != next ? copy(newNext) : (T) this;
	}

	/**
	 * Remove the first element with the given property and value.
	 * 
	 * @param property
	 *            The property whose element should be removed
	 * @param value
	 *            The value whose element should be removed
	 * @return A copy of this data list where the first corresponding has been
	 *         removed.
	 */
	@SuppressWarnings("unchecked")
	public T removeFirst(URI property, Object value) {
		if (this.property == null || property == null || value == null) {
			return (T) this;
		}
		if (property.equals(this.property) && value.equals(this.value)) {
			return next;
		}
		T newNext = next == null ? null : next.removeFirst(property, value);
		return newNext != next ? copy(newNext) : (T) this;
	}

	/**
	 * Returns the size of this list.
	 * 
	 * @return The size of this list
	 */
	public int size() {
		if (property == null) {
			return 0;
		}
		return 1 + (next == null ? 0 : next.size());
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		for (Iterator<T> it = this.iterator(); it.hasNext();) {
			Data<?> d = it.next();
			sb.append("(").append(d.getProperty()).append(", ");
			Object value = d.getValue();
			if (value instanceof Object[]) {
				sb.append(Arrays.toString((Object[])value));
			} else {
				sb.append(value);
			}
			sb.append(")");
			if (it.hasNext()) {
				sb.append(", ");
			}
		}
		return sb.append("]").toString();
	}

	protected abstract T NULL();

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Data)) return false;
		Data<?> data = (Data<?>) o;
		return property.equals(data.property) && Objects.equals(next, data.next) && Objects.equals(value, data.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(property, next, value);
	}
}