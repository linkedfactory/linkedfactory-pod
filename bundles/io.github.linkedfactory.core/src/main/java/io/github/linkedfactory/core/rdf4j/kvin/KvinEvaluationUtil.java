package io.github.linkedfactory.core.rdf4j.kvin;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.rdf4j.common.BNodeWithValue;
import io.github.linkedfactory.core.rdf4j.common.query.AsyncIterator;
import io.github.linkedfactory.core.rdf4j.common.query.CompositeBindingSet;
import io.github.linkedfactory.core.rdf4j.kvin.query.Parameters;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

import java.lang.reflect.Parameter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static io.github.linkedfactory.core.rdf4j.common.Conversions.getLongValue;
import static io.github.linkedfactory.core.rdf4j.common.Conversions.toRdfValue;
import static io.github.linkedfactory.core.rdf4j.kvin.KvinEvaluationStrategy.getVarValue;

public class KvinEvaluationUtil {

	private final Kvin kvin;
	private final Supplier<ExecutorService> executorService;

	public KvinEvaluationUtil(Kvin kvin, Supplier<ExecutorService> executorService) {
		this.kvin = kvin;
		this.executorService = executorService;
	}

	public static net.enilink.komma.core.URI toKommaUri(Value value) {
		if (value instanceof IRI) {
			String valueStr = value.toString();
			// -> KVIN should not allow relative URIs in the future
			// remove scheme for relative URIs
			if (valueStr.startsWith("r:")) {
				valueStr = valueStr.substring(2);
			}
			return URIs.createURI(valueStr);
		}
		return null;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			ValueFactory vf,
			BindingSet bs, Parameters params, StatementPattern stmt, Dataset dataset) {
		net.enilink.komma.core.URI item = toKommaUri(getVarValue(stmt.getSubjectVar(), bs));
		if (item == null) {
			return new EmptyIteration<>();
		}

		ParameterValues pv = new ParameterValues(params, bs);

		// if one of the required parameters is not bound then return an empty iteration
		if (!pv.hasRequiredParameters(params)) {
			return new EmptyIteration<>();
		}

		final Var predVar = stmt.getPredicateVar();
		final Value predValue = getVarValue(predVar, bs);
		final Var contextVar = stmt.getContextVar();
		final Value contextValue = contextVar != null ? getVarValue(contextVar, bs) : null;
		return evaluate(vf, List.of(item), List.of(toKommaUri(predValue)),
				toKommaUri(contextValue), pv, params, bs, stmt, dataset);
	}

	protected CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			ValueFactory vf, List<URI> items, List<URI> properties, URI context,
			ParameterValues pv, Parameters params, BindingSet baseBindings, StatementPattern stmt, Dataset dataset) {
		// the value of item is already known at this point
		// if item is null then it would have to be fetched from the
		// value store, i.e. all available items must be traversed
		// with getDescendants(...)
		final Var objectVar = stmt.getObjectVar();
		final Var contextVar = stmt.getContextVar();

		final net.enilink.komma.core.URI[] finalContext = {context};
		final Value contextValue[] = {null};
		if (finalContext[0] == null && dataset.getDefaultGraphs().isEmpty()) {
			finalContext[0] = Kvin.DEFAULT_CONTEXT;
			contextValue[0] = vf.createIRI(finalContext[0].toString());
		}

		long begin = getLongValue(pv.fromValue, 0L);
		long end = getLongValue(pv.toValue, KvinTuple.TIME_MAX_VALUE);
		long limit = getLongValue(pv.limitValue, 0);
		final long interval = getLongValue(pv.intervalValue, 0);

		final String aggregationFunc;
		if (params.aggregationFunction != null) {
			aggregationFunc = pv.aggregationFuncValue instanceof IRI ? ((IRI) pv.aggregationFuncValue).getLocalName() :
					pv.aggregationFuncValue.stringValue();
		} else {
			aggregationFunc = null;
		}

		Var time = params.time;
		// do not use time as start and end if an aggregation func is used
		// since it leads to wrong/incomplete results
		if (time != null && aggregationFunc == null) {
			// use time as begin and end
			if (pv.timeValue instanceof Literal) {
				long timestamp = ((Literal) pv.timeValue).longValue();
				begin = timestamp;
				end = timestamp;
				// do not set a limit, as multiple values may exist for the same point in time with different sequence numbers
				limit = 0;
			} else {
				// invalid value for time, e.g. an IRI
			}
		}

		Integer seqNrValueInt = pv.seqNrValue == null ? null : ((Literal) pv.seqNrValue).intValue();

		final Var subjectVar = stmt.getSubjectVar();
		final Var predVar = stmt.getPredicateVar();
		final long beginFinal = begin, endFinal = end, limitFinal = limit;
		final CloseableIteration<BindingSet, QueryEvaluationException> iteration = new AbstractCloseableIteration<BindingSet, QueryEvaluationException>() {
			final Thread creator = Thread.currentThread();
			IExtendedIterator<KvinTuple> it;
			URI currentProperty;
			Value currentPropertyValue;
			int index;
			BindingSet next;
			boolean skipProperty;

			@Override
			public boolean hasNext() throws QueryEvaluationException {
				if (next != null) {
					return true;
				}
				if (it == null && !isClosed()) {
					// System.out.println("item=" + item + " property=" + currentProperty + " bindings=" + bs);

					// create iterator with values for property
					if (finalContext[0] != null) {
						it = kvin.fetch(items, properties, finalContext[0], endFinal, beginFinal, limitFinal, interval, aggregationFunc);
					} else {
						for (IRI defaultGraph : dataset.getDefaultGraphs()) {
							URI contextUri = toKommaUri(defaultGraph);
							it = kvin.fetch(items, properties, contextUri, endFinal, beginFinal, limitFinal, interval, aggregationFunc);
							if (it.hasNext()) {
								break;
							}
							it.close();
							it = null;
						}
					}
				}
				if (it != null) {
					if (isClosed()) {
						// close underlying iterator if iteration was closed asynchronously (due to query timeout)
						handleClose();
						return false;
					}
					next = computeNext();
					if (isClosed()) {
						// close underlying iterator if iteration was closed asynchronously (due to query timeout)
						handleClose();
						next = null;
					}
				}
				return next != null;
			}

			@Override
			public BindingSet next() throws QueryEvaluationException {
				if (next == null) {
					throw new NoSuchElementException();
				}
				BindingSet result = next;
				next = null;
				return result;
			}

			BindingSet computeNext() {
				while (it.hasNext()) {
					KvinTuple tuple = it.next();
					// check if current property is changed
					if (!tuple.property.equals(currentProperty)) {
						// reset index
						index = -1;

						currentProperty = tuple.property;
						currentPropertyValue = (IRI) toRdfValue(currentProperty, vf);

						// filters tuples by property if endpoint does not support the filtering by property
						skipProperty = !properties.isEmpty() && !properties.contains(currentProperty);
					}

					// filters tuples by property if endpoint does not support the filtering by property
					if (skipProperty) {
						continue;
					}

					// adds a zero-based index to each returned tuple
					index++;

					// filters any tuple that does not match the requested seqNr, if any
					if (seqNrValueInt != null && seqNrValueInt.intValue() != tuple.seqNr) {
						continue;
					}

					// filters any tuple that does not match the requested index, if any
					if (pv.indexValue != null && (!pv.indexValue.isLiteral() || ((Literal) pv.indexValue).intValue() != index)) {
						continue;
					}

					CompositeBindingSet newBs = new CompositeBindingSet(baseBindings);
					if (!subjectVar.isConstant() && !baseBindings.hasBinding(subjectVar.getName())) {
						Value subjectValue = toRdfValue(tuple.item, vf);
						newBs.addBinding(subjectVar.getName(), subjectValue);
					}
					if (!objectVar.isConstant() && !baseBindings.hasBinding(objectVar.getName())) {
						Value objectValue = BNodeWithValue.create(tuple);
						newBs.addBinding(objectVar.getName(), objectValue);
					}
					if (!predVar.isConstant()) {
						newBs.addBinding(predVar.getName(), currentPropertyValue);
					}
					if (time != null && !time.isConstant() && !baseBindings.hasBinding(time.getName())) {
						newBs.addBinding(time.getName(), toRdfValue(tuple.time, vf));
					}
					if (contextVar != null && !contextVar.isConstant()) {
						newBs.addBinding(contextVar.getName(), contextValue[0]);
					}
					if (params.seqNr != null && pv.seqNrValue == null) {
						newBs.addBinding(params.seqNr.getName(), toRdfValue(tuple.seqNr, vf));
					}
					if (params.index != null && pv.indexValue == null) {
						newBs.addBinding(params.index.getName(), toRdfValue(index, vf));
					}

					return newBs;
				}
				close();
				return null;
			}

			@Override
			public void remove() throws QueryEvaluationException {
				throw new UnsupportedOperationException();
			}

			@Override
			protected void handleClose() throws QueryEvaluationException {
				// do only directly close if current thread is creator
				if (it != null && Thread.currentThread() == creator) {
					it.close();
					it = null;
				}
			}
		};
		return iteration;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			ValueFactory vf, List<BindingSet> bindingSets, Parameters params,
			StatementPattern stmt, Dataset dataset) {
		final Var subjectVar = stmt.getSubjectVar();
		final Var predVar = stmt.getPredicateVar();
		final Iterator<BindingSet> bindingSetIterator = bindingSets.iterator();
		return new AbstractCloseableIteration<>() {
			ParameterValues pv;
			Value contextValue;
			List<CloseableIteration<BindingSet, QueryEvaluationException>> iterators = new ArrayList<>(5);
			CloseableIteration<BindingSet, QueryEvaluationException> it;
			BindingSet last, savedNext;
			Map<Value, Set<Value>> currentItems = new LinkedHashMap<>();

			@Override
			public boolean hasNext() throws QueryEvaluationException {
				if (it == null || !it.hasNext()) {
					if (it != null) {
						it.close();
						it = null;
					}
					if (! iterators.isEmpty()) {
						it = iterators.remove(0);
					}
					while (iterators.size() < 5) {
						if (currentItems.isEmpty()) {
							pv = null;
							contextValue = null;
							while (savedNext != null || bindingSetIterator.hasNext()) {
								BindingSet bs = savedNext != null ? savedNext : bindingSetIterator.next();
								savedNext = null;

								var itemValue = getVarValue(subjectVar, bs);
								if (itemValue == null) {
									it = new EmptyIteration<>();
								}

								final Value predValue = getVarValue(predVar, bs);

								ParameterValues currentPv = new ParameterValues(params, bs);
								Value currentContext = getVarValue(stmt.getContextVar(), bs);
								if ((pv == null || currentPv.equals(pv)) && (contextValue == null || contextValue.equals(currentContext))) {
									var itemProperties = currentItems.computeIfAbsent(itemValue, v -> new LinkedHashSet<>());
									if (predValue != null) {
										itemProperties.add(predValue);
									}
									if (pv == null) {
										pv = currentPv;
									}
									if (contextValue == null) {
										contextValue = currentContext;
									}
									last = bs;
								} else {
									savedNext = bs;
									break;
								}
							}
						}

						while (!currentItems.isEmpty() && iterators.size() < 5) {
							List<URI> items = new ArrayList<>();
							List<URI> properties = new ArrayList<>();
							Set<Value> propertyValues = null;
							for (var itemsIt = currentItems.entrySet().iterator(); itemsIt.hasNext(); ) {
								var entry = itemsIt.next();
								if (propertyValues != null && !entry.getValue().equals(propertyValues)) {
									break;
								}
								itemsIt.remove();
								propertyValues = entry.getValue();
								items.add(toKommaUri(entry.getKey()));
							}
							for (Value propertyValue : propertyValues) {
								properties.add(toKommaUri(propertyValue));
							}

							QueryBindingSet baseBindings = new QueryBindingSet(last);
							if (!subjectVar.isConstant()) {
								baseBindings.removeBinding(subjectVar.getName());
							}
							if (!predVar.isConstant()) {
								baseBindings.removeBinding(predVar.getName());
							}

							if (it == null) {
								it = evaluate(vf, items, properties,
										toKommaUri(contextValue), pv, params, baseBindings, stmt, dataset);
							} else {
								iterators.add(new AsyncIterator<>(() -> evaluate(vf, items, properties,
										toKommaUri(contextValue), pv, params, baseBindings, stmt, dataset),
										executorService));
							}
						}
						if (currentItems.isEmpty()) {
							break;
						}
					}
				}
				return it != null && it.hasNext();
			}

			@Override
			public BindingSet next() throws QueryEvaluationException {
				if (hasNext()) {
					return it.next();
				}
				throw new NoSuchElementException();
			}

			@Override
			public void remove() throws QueryEvaluationException {
				throw new UnsupportedOperationException();
			}

			@Override
			protected void handleClose() throws QueryEvaluationException {
				if (it != null) {
					it.close();
				}
			}
		};
	}

	static class ParameterValues {
		final Value fromValue;
		final Value toValue;
		final Value limitValue;
		final Value intervalValue;
		final Value aggregationFuncValue;
		final Value seqNrValue;
		final Value indexValue;
		final Value timeValue;

		ParameterValues(Parameters params, BindingSet bs) {
			fromValue = getVarValue(params.from, bs);
			toValue = getVarValue(params.to, bs);
			limitValue = getVarValue(params.limit, bs);
			intervalValue = getVarValue(params.interval, bs);
			aggregationFuncValue = getVarValue(params.aggregationFunction, bs);
			seqNrValue = getVarValue(params.seqNr, bs);
			indexValue = getVarValue(params.index, bs);
			timeValue = getVarValue(params.time, bs);
		}

		boolean hasRequiredParameters(Parameters params) {
			return !(params.from != null && fromValue == null || params.to != null && toValue == null
					|| params.limit != null && limitValue == null || params.interval != null && intervalValue == null
					|| params.aggregationFunction != null && aggregationFuncValue == null);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ParameterValues that = (ParameterValues) o;
			return Objects.equals(fromValue, that.fromValue) && Objects.equals(toValue, that.toValue) && Objects.equals(limitValue, that.limitValue) && Objects.equals(intervalValue, that.intervalValue) && Objects.equals(aggregationFuncValue, that.aggregationFuncValue) && Objects.equals(seqNrValue, that.seqNrValue) && Objects.equals(indexValue, that.indexValue) && Objects.equals(timeValue, that.timeValue);
		}

		@Override
		public int hashCode() {
			return Objects.hash(fromValue, toValue, limitValue, intervalValue, aggregationFuncValue, seqNrValue, indexValue, timeValue);
		}
	}
}
