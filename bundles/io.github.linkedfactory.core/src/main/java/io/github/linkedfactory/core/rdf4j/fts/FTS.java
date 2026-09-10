package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public interface FTS {
	String FTS = "fts:";

	IRI KEYWORDS = SimpleValueFactory.getInstance().createIRI(FTS + "keywords");
	IRI SCORE = SimpleValueFactory.getInstance().createIRI(FTS + "score");
	IRI SNIPPET = SimpleValueFactory.getInstance().createIRI(FTS + "snippet");

	IRI FIELD = SimpleValueFactory.getInstance().createIRI(FTS + "field");
	IRI LIMIT = SimpleValueFactory.getInstance().createIRI(FTS + "limit");
	IRI BOOST = SimpleValueFactory.getInstance().createIRI(FTS + "boost");
}
