/*
 * Copyright (c) 2026 Fraunhofer IWU.
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
package io.github.linkedfactory.core.rdf4j.common.query;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.AbstractBindingSet;
import org.eclipse.rdf4j.query.Binding;

/**
 * A compact, immutable-style linked list of (name, value) binding pairs.
 * Duplicate names are explicitly permitted (so a name may occur multiple
 * times). The list is implemented as a small singly-linked structure where
 * each element holds a name, a value and a reference to the next element.
 * <p>
 * This class is inspired by RDF and the scala.xml.MetaData implementation but
 * optimized for a small-footprint representation of query result bindings.
 * <p>
 * Notes:
 * - The sentinel value {@link #NULL} represents an empty list and has all
 * fields set to {@code null}.
 * - Most modifying operations return a new head for the (possibly modified)
 * list rather than mutating the existing nodes.
 */
public class CompactBindingSet extends AbstractBindingSet implements Binding {
	/**
	 * A sentinel representing the empty list. It contains no name or value and
	 * its {@code next} reference is {@code null}.
	 */
	public static final CompactBindingSet NULL = new CompactBindingSet(null, null, null);

	protected final String name;
	protected final CompactBindingSet next;
	protected final Value value;

	/**
	 * Create a new list element.
	 *
	 * @param name  the binding name for this element, may be {@code null} for the
	 *              sentinel/empty element
	 * @param value the RDF4J value associated with the name
	 * @param next  the next list element (may be {@code null})
	 */
	public CompactBindingSet(String name, Value value, CompactBindingSet next) {
		this.name = name;
		this.value = value;
		this.next = next;
	}

	/**
	 * Create a shallow copy of this node with a different {@code next} pointer.
	 * The contained {@link Value} is not cloned.
	 *
	 * @param next the new next element
	 * @return a copy of this node that references {@code next}
	 */
	protected CompactBindingSet copy(CompactBindingSet next) {
		return new CompactBindingSet(this.name, this.value, next);
	}

	/**
	 * Find the first list element with the given name.
	 *
	 * @param name the binding name to search for (null-safe)
	 * @return the first matching element or {@link #NULL} if no element matches
	 */
	public CompactBindingSet first(String name) {
		if (name != null) {
			CompactBindingSet candidate = this;
			while (candidate != null) {
				if (name.equals(candidate.name)) {
					return candidate;
				}
				candidate = candidate.next;
			}
		}
		return NULL;
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
	 * Returns the name of this element. May be {@code null} for the empty
	 * sentinel element.
	 *
	 * @return the binding name or {@code null}
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the RDF4J value of this element. May be {@code null} for the
	 * empty sentinel element.
	 *
	 * @return the value or {@code null}
	 */
	public Value getValue() {
		return value;
	}

	@Override
	public Iterator<Binding> iterator() {
		if (name == null) {
			return Collections.emptyIterator();
		}
		return new Iterator<>() {
			CompactBindingSet next = CompactBindingSet.this;
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
				if (current.next.name == null) {
					next = null;
				} else {
					next = current.next;
				}
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
	 * Returns a sequential {@link Stream} of the contained {@link Binding}s.
	 *
	 * @return a stream over the list elements
	 */
	public Stream<Binding> stream() {
		return StreamSupport.stream(this.spliterator(), false);
	}

	/**
	 * Remove the first element with the given name and return the new head.
	 * This method does not modify existing nodes in place; it creates copies
	 * when necessary and returns the head of the resulting list.
	 *
	 * @param name the binding name to remove
	 * @return the new head of the list after removal
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

	@Override
	public Set<String> getBindingNames() {
		return stream().map(Binding::getName).collect(Collectors.toSet());
	}

	/**
	 * Returns the number of elements in this list. The sentinel/empty element
	 * has size 0.
	 *
	 * @return the size of the list
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
		for (Iterator<Binding> it = this.iterator(); it.hasNext(); ) {
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
}