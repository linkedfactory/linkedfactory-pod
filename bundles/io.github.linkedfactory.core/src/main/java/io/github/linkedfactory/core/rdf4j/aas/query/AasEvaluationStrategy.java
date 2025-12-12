package io.github.linkedfactory.core.rdf4j.aas.query;

import io.github.linkedfactory.core.rdf4j.aas.AAS;
import io.github.linkedfactory.core.rdf4j.aas.AasClient;
import io.github.linkedfactory.core.rdf4j.common.query.CompositeBindingSet;
import io.github.linkedfactory.core.rdf4j.common.query.InnerJoinIteratorEvaluationStep;
import io.github.linkedfactory.core.rdf4j.kvin.query.KvinFetch;
import net.enilink.commons.iterator.IExtendedIterator;
import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext.Minimal;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.HashJoinIteration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static io.github.linkedfactory.core.rdf4j.common.query.Helpers.findFirstFetch;

public class AasEvaluationStrategy extends DefaultEvaluationStrategy {

	static final Resource[] EMPTY_CONTEXTS = new Resource[0];
	final AasClient client;
	final ParameterScanner scanner;
	final ValueFactory vf;
	final Supplier<ExecutorService> executorService;
	final Model cache;

	public AasEvaluationStrategy(AasClient client, Supplier<ExecutorService> executorService, ParameterScanner scanner,
	                             ValueFactory vf, Dataset dataset, FederatedServiceResolver serviceResolver) {
		super(new AasTripleSource(vf), dataset, serviceResolver);
		this.client = client;
		this.cache = client.getCache();
		this.scanner = scanner;
		this.vf = vf;
		this.executorService = executorService;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(StatementPattern stmt, final BindingSet bs)
			throws QueryEvaluationException {
		// System.out.println("Stmt: " + stmt);
		final Var subjectVar = stmt.getSubjectVar();
		final Value subjectValue = getVarValue(subjectVar, bs);
		final Var objectVar = stmt.getObjectVar();
		final Value objectValue = getVarValue(objectVar, bs);
		final Value predValue = getVarValue(stmt.getPredicateVar(), bs);

		if (AAS.API_RESOLVED.equals(predValue)) {
			AAS.ResolvedValue resolved = AAS.resolveReference(cache, (Resource) subjectValue, vf);
			if (resolved != null && resolved.type.equals("Submodel")) {
				// retrieve submodel
				String submodelId = AAS.decodeUri(resolved.value.stringValue());
				try {
					// ensure that submodel exists
					client.submodel(submodelId, true).toList();
				} catch (URISyntaxException | IOException e) {
					throw new QueryEvaluationException(e);
				}
			}
			if (resolved == null || objectValue != null && !objectValue.equals(resolved.value)) {
				return new EmptyIteration<>();
			}
			CompositeBindingSet newBs = new CompositeBindingSet(bs);
			if (objectValue == null) {
				newBs.addBinding(objectVar.getName(), resolved.value);
			}
			return new SingletonIteration<>(newBs);
		}

		if (subjectValue != null && !subjectValue.isResource() || predValue != null && !predValue.isIRI()) {
			return new EmptyIteration<>();
		}

		Value ctxValue = getVarValue(stmt.getContextVar(), bs);
		var stmts = cache.filter((Resource) subjectValue, (IRI) predValue, objectValue,
				ctxValue == null ? EMPTY_CONTEXTS : new Resource[] { (Resource) ctxValue }).iterator();
		if (stmts.hasNext()) {
			return new AbstractCloseableIteration<>() {
				CompositeBindingSet next;

				@Override
				public boolean hasNext() throws QueryEvaluationException {
					while (next == null && stmts.hasNext()) {
						Statement s = stmts.next();
						if (subjectValue != null && !subjectValue.equals(s.getSubject())) {
							// try next value
							continue;
						}
						if (predValue != null && !predValue.equals(s.getPredicate())) {
							// try next value
							continue;
						}
						if (objectValue != null && !objectValue.equals(s.getObject())) {
							// try next value
							continue;
						}

						CompositeBindingSet newBs = new CompositeBindingSet(bs);
						if (subjectValue == null) {
							newBs.addBinding(subjectVar.getName(), s.getSubject());
						}
						if (predValue == null) {
							newBs.addBinding(stmt.getPredicateVar().getName(), s.getPredicate());
						}
						if (objectValue == null) {
							newBs.addBinding(objectVar.getName(), s.getObject());
						}
						next = newBs;
					}
					return next != null;
				}

				@Override
				public BindingSet next() throws QueryEvaluationException {
					if (next == null) {
						hasNext();
					}
					if (next == null) {
						throw new QueryEvaluationException("No such element");
					}
					BindingSet result = next;
					next = null;
					return result;
				}

				@Override
				public void remove() throws QueryEvaluationException {
					throw new UnsupportedOperationException();
				}
			};
		}

		// retrieve shell if IRI starts with urn:aas:AssetAdministrationShell:
		if (subjectValue != null && subjectValue.isIRI() &&
				subjectValue.stringValue().startsWith(AAS.ASSETADMINISTRATIONSHELL_PREFIX)) {
			String shellId = subjectValue.stringValue().substring(AAS.ASSETADMINISTRATIONSHELL_PREFIX.length());
			try (IExtendedIterator<IRI> it = client.shell(shellId, false)) {
				IRI shell = it.next();
				QueryBindingSet newBs = new QueryBindingSet(bs);
				newBs.removeBinding(subjectVar.getName());
				newBs.addBinding(subjectVar.getName(), shell);
				return evaluate(stmt, newBs);
			} catch (URISyntaxException | IOException e) {
				throw new QueryEvaluationException(e);
			}
		}
		return new EmptyIteration<>();
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateFetch(BindingSet bs, Parameters params, StatementPattern stmt) {
		final Var predVar = stmt.getPredicateVar();
		final Var objectVar = stmt.getObjectVar();

		final Value subjValue = getVarValue(stmt.getSubjectVar(), bs);
		final Value predValue = getVarValue(predVar, bs);

		if (subjValue != null) {
			final CloseableIteration<BindingSet, QueryEvaluationException> iteration = new AbstractCloseableIteration<>() {
				IExtendedIterator<IRI> it;

				@Override
				public boolean hasNext() throws QueryEvaluationException {
					if (it == null && !isClosed()) {
						try {
							if (AAS.API_SHELLS.equals(predValue)) {
								it = client.shells();
							} else if (AAS.API_SUBMODELS.equals(predValue)) {
								it = client.submodels();
							}
						} catch (URISyntaxException | IOException e) {
							throw new QueryEvaluationException(e);
						}
					}
					return it != null && it.hasNext();
				}

				@Override
				public BindingSet next() throws QueryEvaluationException {
					var value = it.next();
					CompositeBindingSet newBs = new CompositeBindingSet(bs);
					if (!objectVar.isConstant() && !bs.hasBinding(objectVar.getName())) {
						newBs.addBinding(objectVar.getName(), value);
					}
					return newBs;
				}

				@Override
				public void remove() throws QueryEvaluationException {
					throw new UnsupportedOperationException();
				}

				@Override
				protected void handleClose() throws QueryEvaluationException {
					if (it != null) {
						it.close();
						it = null;
					}
					super.handleClose();
				}
			};
			return iteration;
		}
		return new EmptyIteration<>();
	}

	@Override
	protected QueryEvaluationStep prepare(LeftJoin join, QueryEvaluationContext context) throws QueryEvaluationException {
		if (useHashJoin(join.getLeftArg(), join.getRightArg())) {
			return bindingSet -> new HashJoinIteration(AasEvaluationStrategy.this, join.getLeftArg(), join.getRightArg(), bindingSet, true);
		} else {
			return super.prepare(join, context);
		}
	}

	@Override
	protected QueryEvaluationStep prepare(Join join, QueryEvaluationContext context) throws QueryEvaluationException {
		QueryEvaluationStep leftPrepared = precompile(join.getLeftArg(), context);
		QueryEvaluationStep rightPrepared = precompile(join.getRightArg(), context);
		if (useHashJoin(join.getLeftArg(), join.getRightArg())) {
			String[] joinAttributes = HashJoinIteration.hashJoinAttributeNames(join);
			return bindingSet -> new HashJoinIteration(leftPrepared, rightPrepared, bindingSet, false, joinAttributes, context);
		} else {
			// strictly use lateral joins if left arg contains a AAS fetch as right arg probably depends on the results
			AasFetch fetch = (AasFetch) findFirstFetch(join.getLeftArg());
			boolean lateral = fetch != null;
			// do not use lateral join if left fetch requires a binding from the right join argument
			if (lateral) {
				// switch join order if left depends on right
				Set<String> assured = join.getRightArg().getAssuredBindingNames();
				boolean leftDependsOnRight = fetch.getRequiredBindings().stream()
						.anyMatch(name -> assured.contains(name));
				if (leftDependsOnRight) {
					// swap left and right argument
					return new InnerJoinIteratorEvaluationStep(AasEvaluationStrategy.this,
							executorService, rightPrepared, leftPrepared, true, false
					);
				}
			}
			return new InnerJoinIteratorEvaluationStep(AasEvaluationStrategy.this,
					executorService, leftPrepared, rightPrepared, lateral, false
			);
		}
	}

	boolean useHashJoin(TupleExpr leftArg, TupleExpr rightArg) {
		if (findFirstFetch(leftArg) != null) {
			KvinFetch rightFetch = rightArg instanceof KvinFetch ? (KvinFetch) rightArg : null;
			while (rightArg instanceof Join && rightFetch == null) {
				if (((Join) rightArg).getLeftArg() instanceof KvinFetch) {
					rightFetch = (KvinFetch) ((Join) rightArg).getLeftArg();
				} else {
					rightArg = ((Join) rightArg).getLeftArg();
				}
			}
			if (rightFetch != null) {
				// do not use hash join if required bindings are provided by left join argument
				Set<String> leftAssured = leftArg.getAssuredBindingNames();
				return !rightFetch.getRequiredBindings().stream().anyMatch(required -> leftAssured.contains(required));
			}
		}
		return false;
	}

	protected QueryEvaluationStep prepare(StatementPattern node, QueryEvaluationContext context) throws QueryEvaluationException {
		return bindingSet -> evaluate(node, bindingSet);
	}

	@Override
	public QueryEvaluationStep precompile(TupleExpr expr, QueryEvaluationContext context) {
		if (expr instanceof AasFetch) {
			return new AasFetchEvaluationStep(AasEvaluationStrategy.this, (AasFetch) expr);
		}
		return super.precompile(expr, context);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings)
			throws QueryEvaluationException {
		if (expr instanceof AasFetch) {
			QueryEvaluationContext context = new Minimal(this.dataset, this.tripleSource.getValueFactory());
			return precompile(expr, context).evaluate(bindings);
		}
		return super.evaluate(expr, bindings);
	}

	public AasClient getAasClient() {
		return client;
	}

	public ParameterScanner getScanner() {
		return scanner;
	}

	public ValueFactory getValueFactory() {
		return vf;
	}
}