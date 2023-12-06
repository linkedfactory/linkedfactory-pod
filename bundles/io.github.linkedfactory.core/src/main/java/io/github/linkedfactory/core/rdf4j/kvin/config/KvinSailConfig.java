package io.github.linkedfactory.core.rdf4j.kvin.config;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.base.config.BaseSailConfig;
import org.eclipse.rdf4j.sail.config.AbstractDelegatingSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

public class KvinSailConfig extends AbstractDelegatingSailImplConfig {
	public KvinSailConfig() {
		super(KvinSailFactory.SAIL_TYPE);
	}

	@Override
	public Resource export(Model m) {
		Resource implNode = super.export(m);
		// implement if required
		return implNode;
	}

	@Override
	public void parse(Model m, Resource implNode) throws SailConfigException {
		super.parse(m, implNode);
		// implement if required
	}
}