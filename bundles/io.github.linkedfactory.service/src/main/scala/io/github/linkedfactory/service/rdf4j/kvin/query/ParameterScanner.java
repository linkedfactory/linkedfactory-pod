package io.github.linkedfactory.service.rdf4j.kvin.query;

import io.github.linkedfactory.service.rdf4j.kvin.KVIN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class ParameterScanner extends AbstractQueryModelVisitor<RDF4JException> {

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

	@Override
	public void meet(StatementPattern node) throws RDF4JException {
		processGraphPattern(node);
	}

	public Parameters getParameters(Var subject) {
		return parameterIndex.get(subject);
	}

	public Parameters getParameters(StatementPattern pattern) {
		Parameters subjectParams = parameterIndex.get(pattern.getSubjectVar());
		Parameters objectParams = parameterIndex.get(pattern.getObjectVar());
		if (subjectParams == null) {
			return objectParams;
		} else if (objectParams == null) {
			return subjectParams;
		} else {
			return Parameters.combine(objectParams, subjectParams);
		}
	}

	protected Parameters createParameters(Var subject) {
		Parameters params = parameterIndex.get(subject);
		if (params == null) {
			params = new Parameters();
			parameterIndex.put(subject, params);
		}
		return params;
	}

	private void processGraphPattern(StatementPattern sp) throws RDF4JException {
		Var p = sp.getPredicateVar();
		Value pValue = p.getValue();
		final Var o = sp.getObjectVar();
		boolean remove = true;
		if (KVIN.FROM.equals(pValue) ) {
			// <> kvin:from 213123123 .
			createParameters(sp.getSubjectVar()).from =o;
		} else if (KVIN.TO.equals(pValue) ) {
			// <> kvin:to 213123123
			createParameters(sp.getSubjectVar()).to = o;
		} else if (KVIN.LIMIT.equals(pValue)) {
			// <> kvin:limit 2
			createParameters(sp.getSubjectVar()).limit = o;
		} else if (KVIN.INTERVAL.equals(pValue)) {
			// <> kvin:interval 1000
			createParameters(sp.getSubjectVar()).interval = o;
		} else if (KVIN.OP.equals(pValue)) {
			// <> kvin:op kvin:max
			createParameters(sp.getSubjectVar()).aggregationFunction = o;
		} else if (KVIN.TIME.equals(pValue)) {
			// <> kvin:time ?time
			createParameters(sp.getSubjectVar()).time = o;
		} else if (KVIN.SEQNR.equals(pValue)) {
			// <> kvin:seqNr ?seqNr
			createParameters(sp.getSubjectVar()).seqNr = o;
		} else if (KVIN.INDEX.equals(pValue)) {
			// <> kvin:index ?index
			createParameters(sp.getSubjectVar()).index = o;
		} else if (KVIN.PARAMS.equals(pValue)) {
			// can be used to specify default parameters on an item
			// <> kvin:params [ <kvin:limit> 1 ; <kvin:op> "avg" ; ...]
			Parameters params = createParameters(sp.getObjectVar());
			parameterIndex.put(sp.getSubjectVar(), params);
		} else {
			if (KVIN.VALUE.equals(pValue)) {
				// ensure that parameters are created if only kvin:value is present
				createParameters(sp.getSubjectVar());
			}

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