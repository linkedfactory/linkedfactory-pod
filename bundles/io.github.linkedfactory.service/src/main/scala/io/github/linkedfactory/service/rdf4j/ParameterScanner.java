package io.github.linkedfactory.service.rdf4j;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.*;

class ParameterScanner extends AbstractQueryModelVisitor<RDF4JException> {
	static class Parameters {
		Var from;
		Var to;
		Var limit;
		Var interval;
		Var aggregationFunction;
		Var time;
	}

	Map<Var, Parameters> parameterIndex = new HashMap<>();
	Map<Var, List<StatementPattern>> referencedBy = new HashMap<>();

	/**
	 * Extracts the parameters from the given <code>expr</code>.
	 * 
	 * @param expr
	 *            The expression with parameter statements.
	 */
	public void process(TupleExpr expr) throws RDF4JException {
		expr.visit(this);
	}

	/*
	 * service <kvin:> { <some:item> <some:prop> [ kvin:time ?t; kvin:value ?v ;
	 * kvin:from 213123123; kvin:to 232131234] . }
	 */

	@Override
	public void meet(Join node) throws RDF4JException {
		BGPCollector<RDF4JException> collector = new BGPCollector<>(this);
		node.visit(collector);
		processGraphPattern(collector.getStatementPatterns());
	}

	@Override
	public void meet(StatementPattern node) throws RDF4JException {
		processGraphPattern(Collections.singletonList(node));
	}

	public Parameters getParameters(Var subject) {
		Parameters params = parameterIndex.get(subject);
		if (params == null) {
			params = new Parameters();
			parameterIndex.put(subject, params);
		}
		return params;
	}

	private void processGraphPattern(List<StatementPattern> sps) throws RDF4JException {
		for (StatementPattern sp : sps) {
			Var p = sp.getPredicateVar();
			Value pValue = p.getValue();
			final Var o = sp.getObjectVar();
			boolean remove = true;
			if (KVIN.FROM.equals(pValue) ) {
				// <> kvin:from 213123123 .
				getParameters(sp.getSubjectVar()).from =o;
			} else if (KVIN.TO.equals(pValue) ) {
				// <> kvin:to 213123123
				getParameters(sp.getSubjectVar()).to = o;
			} else if (KVIN.LIMIT.equals(pValue)) {
				// <> kvin:limit 2
				getParameters(sp.getSubjectVar()).limit = o;
			} else if (KVIN.INTERVAL.equals(pValue)) {
				// <> kvin:interval 1000
				getParameters(sp.getSubjectVar()).interval = o;
			} else if (KVIN.OP.equals(pValue)) {
				// <> kvin:op kvin:max
				getParameters(sp.getSubjectVar()).aggregationFunction = o;
			} else if (KVIN.TIME.equals(pValue)) {
				// <> kvin:time ?time
				getParameters(sp.getSubjectVar()).time = o;
			} else {
				// normal statement
				remove = false;
				referencedBy.computeIfAbsent(sp.getObjectVar(), v -> new ArrayList<>()).add(sp);
			}
			// remove any meta statements (from, to etc.) sub-statements for
			// properties of values
			if (remove) {
				sp.replaceWith(new SingletonSet());
			}
		}
	}
}