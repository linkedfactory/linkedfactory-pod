package io.github.linkedfactory.service.rdf4j.query;

import io.github.linkedfactory.service.rdf4j.BGPCollector;
import io.github.linkedfactory.service.rdf4j.KVIN;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.*;

public class ParameterScanner extends AbstractQueryModelVisitor<RDF4JException> {
	public static class Parameters implements Cloneable {
		public Var from;
		public Var to;
		public Var limit;
		public Var interval;
		public Var aggregationFunction;
		public Var time;
		public Var seqNr;

		@Override
		protected Parameters clone() throws CloneNotSupportedException {
			return (Parameters) super.clone();
		}
	}

	public final Map<Var, Parameters> parameterIndex = new HashMap<>();
	public final Map<Var, List<StatementPattern>> referencedBy = new HashMap<>();

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
			} else if (KVIN.SEQNR.equals(pValue)) {
				// <> kvin:seqNr ?seqNr
				getParameters(sp.getSubjectVar()).seqNr = o;
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