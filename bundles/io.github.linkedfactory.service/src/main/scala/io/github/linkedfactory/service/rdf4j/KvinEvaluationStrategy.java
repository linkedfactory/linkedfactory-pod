package io.github.linkedfactory.service.rdf4j;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import net.enilink.komma.core.URI;
import io.github.linkedfactory.kvin.Event;
import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;

import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URIs;
import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;

import java.util.LinkedList;

public class KvinEvaluationStrategy extends StrictEvaluationStrategy {
	final Kvin kvin;
	final ParameterScanner scanner;
	final ValueFactory vf;
	final Map<Value, Object> valueToData;

	public KvinEvaluationStrategy(Kvin kvin, ParameterScanner scanner, ValueFactory vf, Dataset dataset,
								  FederatedServiceResolver serviceResolver, Map<Value, Object> valueToData) {
		super(new KvinTripleSource(vf), dataset, serviceResolver);
		this.kvin = kvin;
		this.scanner = scanner;
		this.vf = vf;
		this.valueToData = valueToData;
	}

	protected long getLongValue(Var var, BindingSet bs, long defaultValue) {
		if (var == null) {
			return defaultValue;
		}
		Value v = getVarValue(var, bs);
		if (v instanceof Literal) {
			return ((Literal) v).longValue();
		}
		return defaultValue;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(StatementPattern stmt, final BindingSet bs)
			throws QueryEvaluationException {
//		System.out.println("Stmt: " + stmt);

		final Var subjectVar = stmt.getSubjectVar();
		final Value subjectValue = getVarValue(subjectVar, bs);

		if (subjectValue == null) {
			return new EmptyIteration<>();
		}

		Object data = valueToData.get(subjectValue);
		if (data instanceof KvinTuple) {
			KvinTuple tuple = (KvinTuple) data;
			Value predValue = getVarValue(stmt.getPredicateVar(),  bs);
			if (predValue != null) {
				QueryBindingSet newBs = new QueryBindingSet(bs);
				if (KVIN.VALUE.equals(predValue)) {
					Var valueVar = stmt.getObjectVar();

					Value rdfValue;
					if (tuple.value instanceof Event) {
						// value is an event, create a blank node
						rdfValue = valueVar.isConstant() ? valueVar.getValue() : vf.createBNode();
						valueToData.put(rdfValue, tuple.value);
					} else {
						// convert value to literal
						rdfValue = toRdfValue(tuple.value);
					}

					if (!valueVar.isConstant()) {
						newBs.addBinding(valueVar.getName(), rdfValue);
					}
					return new SingletonIteration<>(newBs);
				} else if (KVIN.TIME.equals(predValue)) {
					Var timeVar = stmt.getObjectVar();
					// System.out.println("Bind: " + timeVar);
					if (!timeVar.isConstant() && !bs.hasBinding(timeVar.getName())) {
						newBs.addBinding(timeVar.getName(), toRdfValue(tuple.time));
					}
					return new SingletonIteration<>(newBs);
				}
				// TODO support other properties
			}
		} else if (data instanceof Event) {
			Value predValue = getVarValue(stmt.getPredicateVar(), bs);
			net.enilink.komma.core.URI predicate = toKommaUri(subjectValue);
			if (predicate != null) {
				Event e = ((Event)data).first(predicate);
				if (e != Event.NULL) {
					Var valueVar = stmt.getObjectVar();
					QueryBindingSet newBs = new QueryBindingSet(bs);
					if (!valueVar.isConstant()) {
						// TODO recurse if getValue() is also an Event
						newBs.addBinding(valueVar.getName(), toRdfValue(e.getValue()));
					}
					return new SingletonIteration<>(newBs);
				}
			}
		} else {
			net.enilink.komma.core.URI item = toKommaUri(subjectValue);
			if (item != null) {
				// the value of item is already known at this point
				// if item is null then it would have to be fetched from the
				// value store, i.e. all available items must be traversed
				// with getDescendants(...)
				final Var predVar = stmt.getPredicateVar();
				final Var objectVar = stmt.getObjectVar();
				final Var contextVar = stmt.getContextVar();

				final Value[] contextValue = {contextVar != null ? getVarValue(contextVar, bs) : null};
				final net.enilink.komma.core.URI[] context = {null};
				if (contextValue[0] != null) {
					context[0] = toKommaUri(contextValue[0]);
				}
				if (context[0] == null) {
					context[0] = Kvin.DEFAULT_CONTEXT;
					contextValue[0] = vf.createIRI(context[0].toString());
				}
				final Value predValue = getVarValue(predVar, bs);
				net.enilink.komma.core.URI pred = toKommaUri(predValue);
				// TODO this is just a hack to cope with relative URIs
				// -> KVIN should not allow relative URIs in the future
				if (pred != null && "r".equals(pred.scheme())) {
					// remove scheme for relative URIs
					pred = URIs.createURI(pred.toString().substring(2));
				}

				ParameterScanner.Parameters params = scanner.getParameters(objectVar);

				long begin = getLongValue(params.from, bs, 0L);
				long end = getLongValue(params.to, bs, KvinTuple.TIME_MAX_VALUE);
				long limit = getLongValue(params.limit, bs, 0);
				final long interval = getLongValue(params.interval, bs, 0);

				final String aggregationFunc;
				if (params.aggregationFunction != null) {
					Value aggregationFuncValue = getVarValue(params.aggregationFunction, bs);
					aggregationFunc = aggregationFuncValue instanceof IRI ? ((IRI)aggregationFuncValue).getLocalName() : null;
				} else {
					aggregationFunc = null;
				}

				Var time = params.time;
				// do not use time as start and end if an aggregation func is used
				// since it leads to wrong/incomplete results
				if (time != null && aggregationFunc == null) {
					Value timeValue = getVarValue(time, bs);

					// use time as begin and end
					if (timeValue instanceof Literal) {
						long timestamp = ((Literal) timeValue).longValue();
						begin = timestamp;
						end = timestamp;
						limit = 1;
					} else {
						// invalid value for time, e.g. an IRI
					}
				}

				final LinkedList<URI> properties = new LinkedList<>();
				if (pred != null) {
					properties.add(pred);
				} else {
					// fetch properties if not already specified
					properties.addAll(kvin.properties(item).toList());
				}

				final long beginFinal = begin, endFinal = end, limitFinal = limit;
				final CloseableIteration<BindingSet, QueryEvaluationException> iteration = new AbstractCloseableIteration<BindingSet, QueryEvaluationException>() {
					IExtendedIterator<KvinTuple> it;
					IRI currentPropertyIRI;

					@Override
					public boolean hasNext() throws QueryEvaluationException {
						boolean hasNext = it == null ? false : it.hasNext();
						if (!hasNext && it != null) {
							it.close();
						}
						if (!hasNext && !properties.isEmpty()) {
							URI currentProperty = properties.remove();
							if (currentProperty.isRelative()) {
								currentPropertyIRI = vf.createIRI("r:" + currentProperty.toString());
							} else {
								currentPropertyIRI = vf.createIRI(currentProperty.toString());
							}
							// create iterator with values for current property
							it = kvin.fetch(item, currentProperty, context[0], endFinal, beginFinal, limitFinal, interval,
									aggregationFunc);
							hasNext = it.hasNext();
							if (!hasNext) {
								it.close();
							}
						}
						return hasNext;
					}

					@Override
					public BindingSet next() throws QueryEvaluationException {
						KvinTuple tuple = it.next();
						QueryBindingSet newBs = new QueryBindingSet(bs);

						Value objectValue = objectVar.isConstant() ? objectVar.getValue() : vf.createBNode();
						valueToData.put(objectValue, tuple);
						if (!objectVar.isConstant()) {
							newBs.addBinding(objectVar.getName(), objectValue);
						}
						if (!predVar.isConstant()) {
							newBs.addBinding(predVar.getName(), currentPropertyIRI);
						}
						if (time != null && !time.isConstant() && !bs.hasBinding(time.getName())) {
							newBs.addBinding(time.getName(), toRdfValue(tuple.time));
						}
						if (contextVar != null && !contextVar.isConstant()) {
							newBs.addBinding(contextVar.getName(), contextValue[0]);
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
						}
					}
				};
				return iteration;
			}
		}
		return super.evaluate(stmt, bs);
	}

	protected Value toRdfValue(Object value) {
		Value rdfValue;
		if (value instanceof URI) {
			rdfValue = vf.createIRI(value.toString());
		} else if (value instanceof Double) {
			rdfValue = vf.createLiteral((Double) value);
		} else if (value instanceof Float) {
			rdfValue = vf.createLiteral((Float) value);
		} else if (value instanceof Integer) {
			rdfValue = vf.createLiteral((Integer) value);
		} else if (value instanceof Long) {
			rdfValue = vf.createLiteral((Long) value);
		} else if (value instanceof BigDecimal) {
			rdfValue = vf.createLiteral((BigDecimal) value);
		} else if (value instanceof BigInteger) {
			rdfValue = vf.createLiteral((BigInteger) value);
		} else {
			rdfValue = vf.createLiteral(value.toString());
		}
		return rdfValue;
	}

	net.enilink.komma.core.URI toKommaUri(Value value) {
		if (value instanceof IRI) {
			return URIs.createURI(value.toString());
		}
		return null;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Join join, BindingSet bindings)
			throws QueryEvaluationException {
		return new KvinJoinIterator(this, join, bindings);
	}
}
