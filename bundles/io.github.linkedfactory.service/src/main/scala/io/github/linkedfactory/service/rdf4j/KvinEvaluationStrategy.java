package io.github.linkedfactory.service.rdf4j;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

import org.eclipse.rdf4j.common.iteration.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleBNode;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class KvinEvaluationStrategy extends StrictEvaluationStrategy {

	final Kvin kvin;
	final ParameterScanner scanner;
	final ValueFactory vf;

	public KvinEvaluationStrategy(Kvin kvin, ParameterScanner scanner, ValueFactory vf, Dataset dataset,
		FederatedServiceResolver serviceResolver, Map<Value, Object> valueToData) {
		super(new KvinTripleSource(vf), dataset, serviceResolver);
		this.kvin = kvin;
		this.scanner = scanner;
		this.vf = vf;
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
			// this happens for patterns like (:subject :property [ <kvin:value> ?someValue ])
			// where [ <kvin:value> ?someValue ] is evaluated first
			// referencedBy contains the pattern (:subject :property [...])
			List<StatementPattern> referencedBy = scanner.referencedBy.get(subjectVar);
			if (referencedBy != null) {
				for (StatementPattern reference : referencedBy) {
					Value altSubjectValue = getVarValue(reference.getSubjectVar(), bs);
					if (altSubjectValue != null) {
						return new KvinIterationWrapper<>(evaluate(reference, bs)) {
							@Override
							public BindingSet next() throws QueryEvaluationException {
								BindingSet tempBs = super.next();
								// tempBs already contains additional bindings computed by evaluate(reference, bs)
								// as a side effect
								CloseableIteration<BindingSet, QueryEvaluationException> it = evaluate(stmt, tempBs);
								try {
									if (it.hasNext()) {
										return it.next();
									}
									return new EmptyBindingSet();
								} finally {
									it.close();
								}
							}
						};
					}
				}
			}
			return new EmptyIteration<>();
		}

		Object data = subjectValue instanceof BNodeWithValue ? ((BNodeWithValue) subjectValue).value : null;
		if (data instanceof KvinTuple) {
			KvinTuple tuple = (KvinTuple) data;
			Value predValue = getVarValue(stmt.getPredicateVar(), bs);
			if (predValue != null) {
				QueryBindingSet newBs = new QueryBindingSet(bs);
				if (KVIN.VALUE.equals(predValue)) {
					Var valueVar = stmt.getObjectVar();

					Value rdfValue;
					if (tuple.value instanceof Record) {
						// value is an event, create a blank node
						rdfValue = valueVar.isConstant() ? valueVar.getValue() : new BNodeWithValue(tuple.value);
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
					Value timeVarValue = getVarValue(timeVar, bs);
					Value timeValue = toRdfValue(tuple.time);
					if (timeVarValue == null) {
						newBs.addBinding(timeVar.getName(), timeValue);
						return new SingletonIteration<>(newBs);
					} else if (timeVarValue.equals(timeValue)) {
						return new SingletonIteration<>(newBs);
					}
					return new EmptyIteration<>();
				} else if (KVIN.SEQNR.equals(predValue)) {
					Var seqNrVar = stmt.getObjectVar();
					Value seqNrVarValue = getVarValue(seqNrVar, bs);
					Value seqNrValue = toRdfValue(tuple.seqNr);
					if (seqNrVarValue == null) {
						newBs.addBinding(seqNrVar.getName(), seqNrValue);
						return new SingletonIteration<>(newBs);
					} else if (seqNrVarValue.equals(seqNrValue)) {
						return new SingletonIteration<>(newBs);
					}
					return new EmptyIteration<>();
				}
				// TODO support other properties
			}
		} else if (data instanceof Record) {
			Value predValue = getVarValue(stmt.getPredicateVar(), bs);
			net.enilink.komma.core.URI predicate = toKommaUri(subjectValue);
			if (predicate != null) {
				Record r = ((Record) data).first(predicate);
				if (r != Record.NULL) {
					Var valueVar = stmt.getObjectVar();
					QueryBindingSet newBs = new QueryBindingSet(bs);
					if (!valueVar.isConstant()) {
						// TODO recurse if getValue() is also a Record
						newBs.addBinding(valueVar.getName(), toRdfValue(r.getValue()));
					}
					return new SingletonIteration<>(newBs);
				}
			}
		} else {
			if (bs.hasBinding(stmt.getObjectVar().getName())) {
				// bindings where already fully computed via scanner.referencedBy
				return new SingletonIteration<>(bs);
			}

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
					aggregationFunc = aggregationFuncValue instanceof IRI ? ((IRI) aggregationFuncValue).getLocalName() : null;
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
						// do not set a limit, as multiple values may exist for the same point in time with different sequence numbers
						limit = 0;
					} else {
						// invalid value for time, e.g. an IRI
					}
				}

				Var seqNr = params.seqNr;
				Value seqNrValue = getVarValue(seqNr, bs);
				Integer seqNrValueInt = seqNrValue == null ? null : ((Literal) seqNrValue).intValue();

				final LinkedList<URI> properties = new LinkedList<>();
				if (pred != null) {
					properties.add(pred);
				} else {
					// fetch properties if not already specified
					properties.addAll(kvin.properties(item).toList());
				}

				final long beginFinal = begin, endFinal = end, limitFinal = limit;
				final CloseableIteration<BindingSet, QueryEvaluationException> iteration = new AbstractCloseableIteration<>() {
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
								currentPropertyIRI = vf.createIRI("r:" + currentProperty);
							} else {
								currentPropertyIRI = vf.createIRI(currentProperty.toString());
							}
							// create iterator with values for current property
							it = kvin.fetch(item, currentProperty, context[0], endFinal, beginFinal, limitFinal, interval,
								aggregationFunc);
							// filters any tuple that does not match the requested seqNr, if any
							// this is required because the fetch() method does not filter on a specific seqNr
							it = new WrappedIterator<>(it) {
								KvinTuple next;

								@Override
								public boolean hasNext() {
									if (next == null) {
										while (super.hasNext()) {
											KvinTuple tuple = super.next();
											if (seqNrValueInt == null || seqNrValueInt.intValue() == tuple.seqNr) {
												next = tuple;
												break;
											}
										}
									}
									return next != null;
								}

								@Override
								public KvinTuple next() {
									KvinTuple result = next;
									next = null;
									return result;
								}
							};

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

						Value objectValue = objectVar.isConstant() ? objectVar.getValue() : new BNodeWithValue(tuple);
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
						if (seqNr != null && seqNrValue == null) {
							newBs.addBinding(seqNr.getName(), toRdfValue(tuple.seqNr));
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

	@Override
	protected QueryEvaluationStep prepare(Join join, QueryEvaluationContext context) throws QueryEvaluationException {
		return new QueryEvaluationStep() {
			@Override
			public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindingSet) {
				return new KvinJoinIterator(KvinEvaluationStrategy.this, join, bindingSet);
			}
		};
	}

	static class BNodeWithValue extends SimpleBNode {

		private static final String uniqueIdPrefix = UUID.randomUUID().toString().replace("-", "");
		private static final AtomicLong uniqueIdSuffix = new AtomicLong();
		Object value;

		BNodeWithValue(Object value) {
			super(generateId());
			this.value = value;
		}

		static String generateId() {
			String var10001 = uniqueIdPrefix;
			return var10001 + uniqueIdSuffix.incrementAndGet();
		}
	}

	// IterationWrapper has a protected constructor
	class KvinIterationWrapper<E, X extends Exception> extends IterationWrapper<E, X> {

		KvinIterationWrapper(Iteration<? extends E, ? extends X> iter) {
			super(iter);
		}
	}
}