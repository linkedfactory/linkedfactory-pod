package io.github.linkedfactory.core.rdf4j.common;

import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.eclipse.rdf4j.model.impl.SimpleIRI;

public class IRIWithValue extends SimpleIRI implements HasValue {
	public static final URI PROPERTY = URIs.createURI("aas-api:uri");

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
