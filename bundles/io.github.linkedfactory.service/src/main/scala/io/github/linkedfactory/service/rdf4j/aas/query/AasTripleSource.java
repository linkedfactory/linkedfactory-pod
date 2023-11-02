package io.github.linkedfactory.service.rdf4j.aas.query;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

class AasTripleSource implements TripleSource {
	final ValueFactory vf;

	AasTripleSource(ValueFactory vf) {
		this.vf = vf;
	}

	@Override
	public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource s, IRI p, Value o,
			Resource... ctx) throws QueryEvaluationException {
		return new EmptyIteration<>();
	}

	@Override
	public ValueFactory getValueFactory() {
		return vf;
	}

}