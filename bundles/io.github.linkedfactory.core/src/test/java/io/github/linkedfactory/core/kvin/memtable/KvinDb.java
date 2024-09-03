package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;

import java.util.ArrayList;
import java.util.List;

public class KvinDb implements Kvin {
	ValueStore valueStore;
	List<TableFile> tableFiles = new ArrayList<>();

	@Override
	public boolean addListener(KvinListener listener) {
		return false;
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		return false;
	}

	@Override
	public void put(KvinTuple... tuples) {

	}

	@Override
	public void put(Iterable<KvinTuple> tuples) {

	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return null;
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
		return null;
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		return 0;
	}

	@Override
	public boolean delete(URI item, URI context) {
		return false;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		return null;
	}

	@Override
	public void close() {

	}
}
