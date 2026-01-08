package io.github.linkedfactory.core.rdf4j.common.query;

import com.google.common.collect.Streams;
import net.enilink.commons.iterator.WrappedIterator;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.AbstractBindingSet;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A binding set that is based on a given binding set while adding additional bindings without copying the existing ones.
 * <p>
 * The creation of the binding set is faster as the creation of {@link org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet}. As a
 * downside all retrieval methods may need to touch the own bindings and the base binding set.
 */
public class CompositeBindingSet extends AbstractBindingSet {

	private final BindingSet other;
	private CompactBindingSet bindings;

	public CompositeBindingSet(BindingSet other) {
		if (other instanceof CompositeBindingSet) {
			// this ensures that not multiple levels are nested and some kind of linked list is build
			this.bindings = ((CompositeBindingSet) other).bindings;
			this.other = ((CompositeBindingSet) other).other;
		} else {
			this.bindings = CompactBindingSet.NULL;
			this.other = other;
		}
	}

	public void addBinding(String name, Value value) {
		this.bindings = new CompactBindingSet(name, value, this.bindings);
	}

	@Override
	public Iterator<Binding> iterator() {
		if (other == null) {
			return this.bindings.iterator();
		}
		return WrappedIterator.create(other.iterator()).andThen(this.bindings.iterator());
	}

	@Override
	public Set<String> getBindingNames() {
		if (bindings.isEmpty()) {
			return other == null ? Collections.emptySet() : other.getBindingNames();
		}
		if (other == null) {
			return bindings.getBindingNames();
		}
		return Streams.concat(other.getBindingNames().stream(), bindings.getBindingNames().stream())
				.collect(Collectors.toSet());
	}

	@Override
	public Binding getBinding(String s) {
		Binding b = bindings.getBinding(s);
		if (b != null) {
			return b;
		}
		return other == null ? null : other.getBinding(s);
	}

	@Override
	public boolean hasBinding(String s) {
		return bindings.hasBinding(s) || other != null && other.hasBinding(s);
	}

	@Override
	public Value getValue(String s) {
		Value v = bindings.getValue(s);
		if (v != null) {
			return v;
		}
		return other == null ? null : other.getValue(s);
	}

	@Override
	public int size() {
		return (other == null ? 0 : other.size()) + bindings.size();
	}
}