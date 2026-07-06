package io.github.linkedfactory.core.rdf4j.fts.config;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.sail.config.AbstractDelegatingSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

import java.util.Optional;

public class FtsSailConfig extends AbstractDelegatingSailImplConfig {
	public static final String CONFIG_NS = "http://linkedfactory.github.io/config/sail/fts#";
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final IRI ENDPOINT = VF.createIRI(CONFIG_NS, "endpoint");
	private static final IRI BULK_PATH = VF.createIRI(CONFIG_NS, "bulkPath");
	private static final IRI FAIL_ON_ERROR = VF.createIRI(CONFIG_NS, "failOnError");

	private String endpoint;
	private String bulkPath = "/fts/bulk";
	private boolean failOnError = true;

	public FtsSailConfig() {
		super(FtsSailFactory.SAIL_TYPE);
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getBulkPath() {
		return bulkPath;
	}

	public void setBulkPath(String bulkPath) {
		if (bulkPath != null && !bulkPath.isBlank()) {
			this.bulkPath = bulkPath;
		}
	}

	public boolean isFailOnError() {
		return failOnError;
	}

	public void setFailOnError(boolean failOnError) {
		this.failOnError = failOnError;
	}

	@Override
	public Resource export(Model model) {
		Resource implNode = super.export(model);
		if (endpoint != null && !endpoint.isBlank()) {
			model.add(implNode, ENDPOINT, VF.createLiteral(endpoint));
		}
		if (bulkPath != null && !bulkPath.isBlank()) {
			model.add(implNode, BULK_PATH, VF.createLiteral(bulkPath));
		}
		model.add(implNode, FAIL_ON_ERROR, VF.createLiteral(failOnError));
		return implNode;
	}

	@Override
	public void parse(Model model, Resource implNode) throws SailConfigException {
		super.parse(model, implNode);

		Optional<Literal> endpointLiteral = Models.objectLiteral(model.filter(implNode, ENDPOINT, null));
		endpointLiteral.ifPresent(literal -> endpoint = literal.getLabel());

		Optional<Literal> bulkPathLiteral = Models.objectLiteral(model.filter(implNode, BULK_PATH, null));
		bulkPathLiteral.ifPresent(literal -> bulkPath = literal.getLabel());

		Optional<Literal> failOnErrorLiteral = Models.objectLiteral(model.filter(implNode, FAIL_ON_ERROR, null));
		if (failOnErrorLiteral.isPresent()) {
			try {
				failOnError = failOnErrorLiteral.get().booleanValue();
			} catch (IllegalArgumentException e) {
				throw new SailConfigException("Invalid boolean for fts:failOnError", e);
			}
		}
	}
}
