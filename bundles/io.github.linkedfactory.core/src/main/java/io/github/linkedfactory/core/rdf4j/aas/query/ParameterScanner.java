package io.github.linkedfactory.core.rdf4j.aas.query;

import io.github.linkedfactory.core.rdf4j.aas.AAS;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.HashMap;
import java.util.Map;

public class ParameterScanner extends AbstractQueryModelVisitor<RDF4JException> {

	public final Map<Var, Parameters> parameterIndex = new HashMap<>();

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
		if (AAS.API_SHELLS.equals(pValue) || AAS.API_SUBMODELS.equals(pValue)) {
			Parameters params = createParameters(sp.getObjectVar());
			parameterIndex.put(sp.getSubjectVar(), params);
			remove = false;
		} else if (AAS.API_PARAMS.equals(pValue)) {
			// can be used to specify default parameters on an item
			// <> aas:params [ <aas:limit> 1 ]
			Parameters params = createParameters(sp.getObjectVar());
			parameterIndex.put(sp.getSubjectVar(), params);
		} else {
			// normal statement
			remove = false;
		}
		// remove any meta statements (from, to etc.) sub-statements for
		// properties of values
		if (remove) {
			sp.replaceWith(new SingletonSet());
		}
	}
}