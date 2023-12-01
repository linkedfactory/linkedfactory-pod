package io.github.linkedfactory.core.rdf4j.common.query;

import net.enilink.commons.iterator.WrappedIterator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.AbstractBindingSet;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.SimpleBinding;

import com.google.common.collect.Streams;

/**
 * A binding set that is based on a given binding set while adding additional bindings without copying the existing ones.
 * <p>
 * The creation of the binding set is faster as the creation of {@link org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet}. As a
 * downside all retrieval methods may need to touch the own bindings and the base binding set.
 */
public class CompositeBindingSet extends AbstractBindingSet {

    private final BindingSet other;
    private final LinkedHashMap<String, Value> bindings;

    public CompositeBindingSet(BindingSet other) {
        if (other instanceof CompositeBindingSet) {
            // this ensures that not multiple levels are nested and some kind of linked list is build
            this.bindings = (LinkedHashMap<String, Value>) ((CompositeBindingSet) other).bindings.clone();
            this.other = ((CompositeBindingSet) other).other;
        } else {
            this.bindings = new LinkedHashMap<>(3);
            this.other = other;
        }
    }

    public void addBinding(String name, Value value) {
        assert !other.hasBinding(name) : "variable already bound: " + name;
        this.bindings.put(name, value);
    }

    @Override
    public Iterator<Binding> iterator() {
        return WrappedIterator.create(other.iterator()).andThen(
            WrappedIterator.create(this.bindings.entrySet().iterator())
                .filterKeep(entry -> entry.getValue() != null)
                .mapWith(entry -> new SimpleBinding(entry.getKey(), entry.getValue())));
    }

    @Override
    public Set<String> getBindingNames() {
        if (bindings.isEmpty()) {
            return other.getBindingNames();
        }
        return Streams.concat(other.getBindingNames().stream(), bindings.keySet().stream())
            .collect(Collectors.toSet());
    }

    @Override
    public Binding getBinding(String s) {
        Binding b = other.getBinding(s);
        if (b != null) {
            return b;
        }
        Value v = bindings.get(s);
        if (v != null) {
            return new SimpleBinding(s, v);
        }
        return null;
    }

    @Override
    public boolean hasBinding(String s) {
        return bindings.containsKey(s) || other.hasBinding(s);
    }

    @Override
    public Value getValue(String s) {
        Value v = bindings.get(s);
        if (v != null) {
            return v;
        }
        return other.getValue(s);
    }

    @Override
    public int size() {
        return other.size() + bindings.size();
    }
}