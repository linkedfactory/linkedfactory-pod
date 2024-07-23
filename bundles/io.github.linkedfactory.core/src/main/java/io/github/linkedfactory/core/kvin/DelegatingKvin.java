package io.github.linkedfactory.core.kvin;

import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import java.util.function.Supplier;

public class DelegatingKvin implements Kvin {
    final Supplier<Kvin> delegateSupplier;

    public DelegatingKvin(Supplier<Kvin> delegateSupplier) {
        this.delegateSupplier = delegateSupplier;
    }

    @Override
    public boolean addListener(KvinListener listener) {
        return getDelegate().addListener(listener);
    }

    @Override
    public boolean removeListener(KvinListener listener) {
        return getDelegate().removeListener(listener);
    }

    @Override
    public void put(KvinTuple... tuples) {
        getDelegate().put(tuples);
    }

    @Override
    public void put(Iterable<KvinTuple> tuples) {
        getDelegate().put(tuples);
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
        return getDelegate().fetch(item, property, context, limit);
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval,
        String op) {
        return getDelegate().fetch(item, property, context, end, begin, limit, interval, op);
    }

    @Override
    public long delete(URI item, URI property, URI context, long end, long begin) {
        return getDelegate().delete(item, property, context, end, begin);
    }

    @Override
    public boolean delete(URI item, URI context) {
        return getDelegate().delete(item, context);
    }

    @Override
    public IExtendedIterator<URI> descendants(URI item, URI context) {
        return getDelegate().descendants(item, context);
    }

    @Override
    public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
        return getDelegate().descendants(item, context, limit);
    }

    @Override
    public IExtendedIterator<URI> properties(URI item, URI context) {
        return getDelegate().properties(item, context);
    }

    @Override
    public void close() {
        getDelegate().close();
    }

    protected Kvin getDelegate() {
        return delegateSupplier.get();
    }
}
