package io.github.linkedfactory.service.rdf4j.kvin;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

class KvinTripleSource implements TripleSource {
	final ValueFactory vf;

	KvinTripleSource(ValueFactory vf) {
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