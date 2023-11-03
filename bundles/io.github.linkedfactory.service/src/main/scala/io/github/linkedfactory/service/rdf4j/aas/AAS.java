package io.github.linkedfactory.service.rdf4j.aas;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public interface AAS {
	String AAS = "aas:";

	IRI PARAMS = SimpleValueFactory.getInstance().createIRI(AAS + "params");

	IRI SHELLS = SimpleValueFactory.getInstance().createIRI(AAS + "shells");
	IRI SUBMODELS = SimpleValueFactory.getInstance().createIRI(AAS + "submodels");
}
