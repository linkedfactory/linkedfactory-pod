package io.github.linkedfactory.service.rdf4j;

import java.util.Map;
import java.util.WeakHashMap;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryOptimizerList;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSailConnection;

public class KvinConnection extends AbstractSailConnection {
	final KvinSail kvinSail;
	final Map<Value, Object> valueToData = new WeakHashMap<>();

	public KvinConnection(KvinSail sail) {
		super(sail);
		kvinSail = sail;
	}

	@Override
	protected void addStatementInternal(Resource s, IRI p, Value o, Resource... ctx) throws SailException {

	}

	@Override
	protected void clearInternal(Resource... ctx) throws SailException {
	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
	}

	@Override
	protected void closeInternal() throws SailException {
	}

	@Override
	protected void commitInternal() throws SailException {
	}

	@Override
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr,
			Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
		// Clone the tuple expression to allow rewriting by optimizers
		tupleExpr = tupleExpr.clone();

		if (!(tupleExpr instanceof QueryRoot)) {
			// Add a dummy root node to the tuple expressions to allow the
			// optimizers to modify the actual root node
			tupleExpr = new QueryRoot(tupleExpr);
		}

		final ParameterScanner scanner = new ParameterScanner();
		try {
			scanner.process(tupleExpr);
		} catch (RDF4JException e) {
			throw new QueryEvaluationException(e);
		}

		QueryOptimizerList optimizers = new QueryOptimizerList();
		optimizers.add(new QueryModelNormalizer());

		EvaluationStrategy strategy = new KvinEvaluationStrategy(kvinSail.kvin, scanner, kvinSail.vf, null, null,
				valueToData);
		optimizers.optimize(tupleExpr, null, bindings);

		return strategy.evaluate(tupleExpr, bindings);
	}

	@Override
	protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal() throws SailException {
		return null;
	}

	@Override
	protected String getNamespaceInternal(String prefix) throws SailException {
		return null;
	}

	@Override
	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal() throws SailException {
		return new EmptyIteration<>();
	}

	@Override
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource s, IRI p, Value o,
			boolean includeInferred, Resource... ctx) throws SailException {
		return new EmptyIteration<>();
	}

	@Override
	protected void removeNamespaceInternal(String prefix) throws SailException {

	}

	@Override
	protected void removeStatementsInternal(Resource s, IRI p, Value o, Resource... ctx) throws SailException {

	}

	@Override
	protected void rollbackInternal() throws SailException {

	}

	@Override
	protected void setNamespaceInternal(String prefix, String name) throws SailException {

	}

	@Override
	protected long sizeInternal(Resource... ctx) throws SailException {
		return 0;
	}

	@Override
	protected void startTransactionInternal() throws SailException {

	}

}
