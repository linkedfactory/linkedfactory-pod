package io.github.linkedfactory.service.rdf4j;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSail;

import io.github.linkedfactory.kvin.Kvin;

public class KvinSail extends AbstractSail {
	final ValueFactory vf = SimpleValueFactory.getInstance();
	final Kvin kvin;

	public KvinSail(Kvin kvin) {
		this.kvin = kvin;
	}

	@Override
	public ValueFactory getValueFactory() {
		return vf;
	}

	@Override
	public boolean isWritable() throws SailException {
		return false;
	}

	@Override
	protected SailConnection getConnectionInternal() throws SailException {
		return new KvinConnection(this);
	}

	@Override
	protected void shutDownInternal() throws SailException {
	}
}
