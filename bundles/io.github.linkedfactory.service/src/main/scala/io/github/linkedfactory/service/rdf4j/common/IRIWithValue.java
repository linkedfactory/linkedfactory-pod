package io.github.linkedfactory.service.rdf4j.common;

import org.eclipse.rdf4j.model.impl.SimpleIRI;

import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class IRIWithValue extends SimpleIRI implements HasValue {

	protected final Object value;

	private IRIWithValue(String iriString, Object value) {
		super(iriString);
		this.value = value;
	}

	public static IRIWithValue create(String iriString, Object value) {
		return new IRIWithValue(iriString, value);
	}

	public Object getValue() {
		return value;
	}
}
