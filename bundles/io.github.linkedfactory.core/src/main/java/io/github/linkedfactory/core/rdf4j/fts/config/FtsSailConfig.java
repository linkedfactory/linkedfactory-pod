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
	private static final IRI BACKEND = VF.createIRI(CONFIG_NS, "backend");
	private static final IRI ENDPOINT = VF.createIRI(CONFIG_NS, "endpoint");
	private static final IRI BULK_PATH = VF.createIRI(CONFIG_NS, "bulkPath");
	private static final IRI SEARCH_PATH = VF.createIRI(CONFIG_NS, "searchPath");
	private static final IRI FAIL_ON_ERROR = VF.createIRI(CONFIG_NS, "failOnError");
	private static final IRI DEFAULT_LIMIT = VF.createIRI(CONFIG_NS, "defaultLimit");

	private String backend = "elastic";
	private String endpoint;
	private String bulkPath = "/fts/bulk";
	private String searchPath = "/fts/_search";
	private boolean failOnError = true;
	private int defaultLimit = 100;

	public FtsSailConfig() {
		super(FtsSailFactory.SAIL_TYPE);
	}

	public String getBackend() {
		return backend;
	}

	public void setBackend(String backend) {
		if (backend != null && !backend.isBlank()) {
			this.backend = backend;
		}
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

	public String getSearchPath() {
		return searchPath;
	}

	public void setSearchPath(String searchPath) {
		if (searchPath != null && !searchPath.isBlank()) {
			this.searchPath = searchPath;
		}
	}

	public int getDefaultLimit() {
		return defaultLimit;
	}

	public void setDefaultLimit(int defaultLimit) {
		this.defaultLimit = Math.max(defaultLimit, 1);
	}

	@Override
	public Resource export(Model model) {
		Resource implNode = super.export(model);
		if (backend != null && !backend.isBlank()) {
			model.add(implNode, BACKEND, VF.createLiteral(backend));
		}
		if (endpoint != null && !endpoint.isBlank()) {
			model.add(implNode, ENDPOINT, VF.createLiteral(endpoint));
		}
		if (bulkPath != null && !bulkPath.isBlank()) {
			model.add(implNode, BULK_PATH, VF.createLiteral(bulkPath));
		}
		if (searchPath != null && !searchPath.isBlank()) {
			model.add(implNode, SEARCH_PATH, VF.createLiteral(searchPath));
		}
		model.add(implNode, FAIL_ON_ERROR, VF.createLiteral(failOnError));
		model.add(implNode, DEFAULT_LIMIT, VF.createLiteral(defaultLimit));
		return implNode;
	}

	@Override
	public void parse(Model model, Resource implNode) throws SailConfigException {
		super.parse(model, implNode);

		Optional<Literal> backendLiteral = Models.objectLiteral(model.filter(implNode, BACKEND, null));
		backendLiteral.ifPresent(literal -> backend = literal.getLabel());

		Optional<Literal> endpointLiteral = Models.objectLiteral(model.filter(implNode, ENDPOINT, null));
		endpointLiteral.ifPresent(literal -> endpoint = literal.getLabel());

		Optional<Literal> bulkPathLiteral = Models.objectLiteral(model.filter(implNode, BULK_PATH, null));
		bulkPathLiteral.ifPresent(literal -> bulkPath = literal.getLabel());

		Optional<Literal> searchPathLiteral = Models.objectLiteral(model.filter(implNode, SEARCH_PATH, null));
		searchPathLiteral.ifPresent(literal -> searchPath = literal.getLabel());

		Optional<Literal> failOnErrorLiteral = Models.objectLiteral(model.filter(implNode, FAIL_ON_ERROR, null));
		if (failOnErrorLiteral.isPresent()) {
			try {
				failOnError = failOnErrorLiteral.get().booleanValue();
			} catch (IllegalArgumentException e) {
				throw new SailConfigException("Invalid boolean for fts:failOnError", e);
			}
		}

		Optional<Literal> defaultLimitLiteral = Models.objectLiteral(model.filter(implNode, DEFAULT_LIMIT, null));
		if (defaultLimitLiteral.isPresent()) {
			try {
				defaultLimit = Math.max(defaultLimitLiteral.get().intValue(), 1);
			} catch (IllegalArgumentException e) {
				throw new SailConfigException("Invalid integer for fts:defaultLimit", e);
			}
		}
	}
}
