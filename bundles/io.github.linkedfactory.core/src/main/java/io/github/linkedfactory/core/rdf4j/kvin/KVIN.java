package io.github.linkedfactory.core.rdf4j.kvin;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public interface KVIN {
	String KVIN = "kvin:";
	IRI KVIN_IRI = SimpleValueFactory.getInstance().createIRI(KVIN);

	IRI FROM = SimpleValueFactory.getInstance().createIRI(KVIN + "from");
	IRI TO = SimpleValueFactory.getInstance().createIRI(KVIN + "to");
	IRI LIMIT = SimpleValueFactory.getInstance().createIRI(KVIN + "limit");

	IRI INTERVAL = SimpleValueFactory.getInstance().createIRI(KVIN + "interval");
	IRI OP = SimpleValueFactory.getInstance().createIRI(KVIN + "op");

	IRI VALUE = SimpleValueFactory.getInstance().createIRI(KVIN + "value");
	IRI VALUE_JSON = SimpleValueFactory.getInstance().createIRI(KVIN + "valueJson");
	IRI TIME = SimpleValueFactory.getInstance().createIRI(KVIN + "time");
	IRI SEQNR = SimpleValueFactory.getInstance().createIRI(KVIN + "seqNr");

	IRI INDEX = SimpleValueFactory.getInstance().createIRI(KVIN + "index");

	IRI PARAMS = SimpleValueFactory.getInstance().createIRI(KVIN + "params");
}
