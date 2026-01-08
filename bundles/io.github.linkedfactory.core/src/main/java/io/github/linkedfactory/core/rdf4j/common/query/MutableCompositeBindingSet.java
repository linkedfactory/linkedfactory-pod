package io.github.linkedfactory.core.rdf4j.common.query;

import java.util.Iterator;
import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;

/**
 * A simple mutable binding set backed by the immutable-style CompactBindingSet linked list.
 * Mutations replace the internal head to keep operations cheap.
 */
public class MutableCompositeBindingSet extends CompositeBindingSet implements MutableBindingSet {
	public MutableCompositeBindingSet(BindingSet other) {
		super(other);
	}

	@Override
	public void addBinding(Binding binding) {
		if (binding == null) return;
		addBinding(binding.getName(), binding.getValue());
	}

	@Override
	public void setBinding(Binding binding) {
		addBinding(binding);
	}

	@Override
	public void setBinding(String name, Value value) {
		addBinding(name, value);
	}
}
