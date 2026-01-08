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
 * WITHOUCompactBindingSet WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.core.rdf4j.common.query;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import net.enilink.komma.core.URI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.AbstractBindingSet;
import org.eclipse.rdf4j.query.Binding;

/**
 * A linked list of (property, value) pairs where properties are {@link URI}s
 * and values are arbitrary objects. Duplicate properties are explicitly
 * permitted.
 * 
 * This is inspired by RDF and the scala.xml.MetaData implementation.
 */
public class CompactBindingSet extends AbstractBindingSet implements Binding {
	protected static final CompactBindingSet NULL = new CompactBindingSet(null, null, null);
	protected final String name;
	protected final CompactBindingSet next;
	protected final Value value;

	public CompactBindingSet(String name, Value value, CompactBindingSet next) {
		this.name = name;
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
	public CompactBindingSet append(CompactBindingSet data) {
		if (this.name == null) {
			return data;
		} else if (this.next == null) {
			return copy(data);
		}
		return next.append(data);
	}

	/**
	 * Create a copy of this data element.
	 * 
	 * @param next
	 *            The new next element
	 * @return A copy of this data element with a new next element.
	 */
	public CompactBindingSet copy(CompactBindingSet next) {
		return new CompactBindingSet(this.name, this.value, next);
	}

	/**
	 * Find the first data element within this list for the given property.
	 * 
	 * @param name
	 *            The name
	 * @return The data element for the given name or an empty data element
	 *         if it was not found.
	 */
	public CompactBindingSet first(String name) {
		if (name != null) {
			if (name.equals(this.name)) {
				return this;
			}
			if (next != null) {
				return next.first(name);
			}
		}
		return NULL();
	}

	@Override
	public Binding getBinding(String s) {
		Binding b = first(s);
		return b != NULL ? b : null;
	}

	@Override
	public boolean hasBinding(String s) {
		return first(s) != NULL;
	}

	@Override
	public Value getValue(String s) {
		Binding b = first(s);
		if (b != NULL) {
			return b.getValue();
		}
		return null;
	}

	/**
	 * Returns the property of this data element.
	 * 
	 * @return The property
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the value of this data element.
	 * 
	 * @return The value
	 */
	public Value getValue() {
		return value;
	}

	@Override
	public Iterator<Binding> iterator() {
		return new Iterator<Binding>() {
			CompactBindingSet next = name != null ? CompactBindingSet.this : null;
			CompactBindingSet current = null;

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public CompactBindingSet next() {
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
				current.removeFirst(current.name);
			}
		};
	}

	/**
	 * Returns a stream of the contained elements.
	 *
	 * @return Stream of the data elements.
	 */
	public Stream<Binding> stream() {
		return StreamSupport.stream(this.spliterator(), false);
	}

	/**
	 * Return the next element.
	 *
	 * @return The next element or {@link #NULL()}
	 */
	public CompactBindingSet next() {
		return next == null ? NULL() : next;
	}

	/**
	 * Remove the first element with the given name.
	 * 
	 * @param name
	 *            The name whose element should be removed
	 * @return A copy of this data list where the first corresponding has been
	 *         removed.
	 */
	public CompactBindingSet removeFirst(String name) {
		if (this.name == null || name == null) {
			return this;
		}
		if (name.equals(this.name)) {
			return next;
		}
		CompactBindingSet newNext = next == null ? null : next.removeFirst(name);
		return newNext != next ? copy(newNext) : this;
	}

	/**
	 * Remove the first element with the given name and value.
	 * 
	 * @param name
	 *            The name whose element should be removed
	 * @param value
	 *            The value whose element should be removed
	 * @return A copy of this data list where the first corresponding element has been
	 *         removed.
	 */
	public CompactBindingSet removeFirst(String name, Object value) {
		if (this.name == null || name == null || value == null) {
			return this;
		}
		if (name.equals(this.name) && value.equals(this.value)) {
			return next;
		}
		CompactBindingSet newNext = next == null ? null : next.removeFirst(name, value);
		return newNext != next ? copy(newNext) : this;
	}

	@Override
	public Set<String> getBindingNames() {
		return stream().map(Binding::getName).collect(Collectors.toSet());
	}

	/**
	 * Returns the size of this list.
	 * 
	 * @return The size of this list
	 */
	public int size() {
		if (name == null) {
			return 0;
		}
		return 1 + (next == null ? 0 : next.size());
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		for (Iterator<Binding> it = this.iterator(); it.hasNext();) {
			Binding d = it.next();
			sb.append("(").append(d.getName()).append(", ");
			Object value = d.getValue();
			sb.append(value);
			sb.append(")");
			if (it.hasNext()) {
				sb.append(", ");
			}
		}
		return sb.append("]").toString();
	}

	protected CompactBindingSet NULL() {
		return NULL;
	}
}