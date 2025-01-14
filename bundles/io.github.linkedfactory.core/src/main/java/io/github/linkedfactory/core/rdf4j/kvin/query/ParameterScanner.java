package io.github.linkedfactory.core.rdf4j.kvin.query;

import io.github.linkedfactory.core.rdf4j.kvin.KVIN;
import net.enilink.commons.util.Pair;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.*;

public class ParameterScanner extends AbstractQueryModelVisitor<RDF4JException> {

	protected final Map<VariableScopeChange, Map<Var, Parameters>> parameterIndex = new IdentityHashMap<>();
	protected final Map<Var, List<StatementPattern>> referencedBy = new HashMap<>();

	/**
	 * Extracts the parameters from the given <code>expr</code>.
	 *
	 * @param expr The expression with parameter statements.
	 */
	public void process(TupleExpr expr) throws RDF4JException {
		expr.visit(this);
	}

	@Override
	public void meet(StatementPattern node) throws RDF4JException {
		processGraphPattern(node);
	}

	public Parameters getParameters(Var subject) {
		return indexFor(subject, false).get(subject);
	}

	/**
	 * Find closest parent (e.g., a sub-select) that indicates a scope change of the given variable.
	 *
	 * @param var The variable
	 * @return closest scope changing node or <code>null</code>
	 */
	protected VariableScopeChange getContext(Var var) {
		QueryModelNode parent = var.getParentNode();
		while (parent != null) {
			if (parent instanceof VariableScopeChange && ((VariableScopeChange) parent).isVariableScopeChange()) {
				return (VariableScopeChange) parent;
			}
			parent = parent.getParentNode();
		}
		return null;
	}

	protected Map<Var, Parameters> indexFor(Var var, boolean create) {
		var context = getContext(var);
		if (create) {
			return parameterIndex.computeIfAbsent(context, c -> new HashMap<>());
		}
		return parameterIndex.getOrDefault(context, Collections.emptyMap());
	}

	public Parameters getParameters(StatementPattern pattern) {
		Parameters subjectParams = indexFor(pattern.getSubjectVar(), false).get(pattern.getSubjectVar());
		Parameters objectParams = indexFor(pattern.getObjectVar(), false).get(pattern.getObjectVar());
		if (subjectParams == null) {
			return objectParams;
		} else if (objectParams == null) {
			return subjectParams;
		} else {
			return Parameters.combine(objectParams, subjectParams);
		}
	}

	protected Parameters createParameters(Var subject) {
		var index = indexFor(subject, true);
		Parameters params = index.get(subject);
		if (params == null) {
			params = new Parameters();
			index.put(subject, params);
		}
		return params;
	}

	private void processGraphPattern(StatementPattern sp) throws RDF4JException {
		Var p = sp.getPredicateVar();
		Value pValue = p.getValue();
		final Var o = sp.getObjectVar();
		boolean remove = true;
		if (KVIN.FROM.equals(pValue)) {
			// <> kvin:from 213123123 .
			createParameters(sp.getSubjectVar()).from = o;
		} else if (KVIN.TO.equals(pValue)) {
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
			var index = indexFor(sp.getSubjectVar(), true);
			index.put(sp.getSubjectVar(), params);
		} else {
			if (KVIN.VALUE.equals(pValue) || KVIN.VALUE_JSON.equals(pValue)) {
				// ensure that parameters are created if only kvin:value or kivn:valueJson is present
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