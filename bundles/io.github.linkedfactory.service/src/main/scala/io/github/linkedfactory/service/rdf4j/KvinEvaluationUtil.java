package io.github.linkedfactory.service.rdf4j;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.service.rdf4j.KvinEvaluationStrategy.BNodeWithValue;
import io.github.linkedfactory.service.rdf4j.query.KvinFetch;
import io.github.linkedfactory.service.rdf4j.query.Parameters;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;

import static io.github.linkedfactory.service.rdf4j.KvinEvaluationStrategy.*;

public class KvinEvaluationUtil {

    private final Kvin kvin;

    public KvinEvaluationUtil(Kvin kvin) {
        this.kvin = kvin;
    }

    public static long getLongValue(Value v, long defaultValue) {
        if (v instanceof Literal) {
            return ((Literal) v).longValue();
        }
        return defaultValue;
    }

    static Value toRdfValue(Object value, ValueFactory vf) {
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
        } else if (value instanceof Record) {
            return new BNodeWithValue(value);
        } else {
            rdfValue = vf.createLiteral(value.toString());
        }
        return rdfValue;
    }

    public static net.enilink.komma.core.URI toKommaUri(Value value) {
        if (value instanceof IRI) {
            return URIs.createURI(value.toString());
        }
        return null;
    }

    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(ValueFactory vf,
        BindingSet bs, Parameters params, StatementPattern stmt) {
        net.enilink.komma.core.URI item = toKommaUri(getVarValue(stmt.getSubjectVar(), bs));
        if (item == null) {
            return new EmptyIteration<>();
        }

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

        Value fromValue = getVarValue(params.from, bs);
        Value toValue = getVarValue(params.to, bs);
        Value limitValue = getVarValue(params.limit, bs);
        Value intervalValue = getVarValue(params.interval, bs);
        Value aggregationFuncValue = getVarValue(params.aggregationFunction, bs);

        // if one of the required parameters is not bound then return an empty iteration
        if (params.from != null && fromValue == null || params.to != null && toValue == null
            || params.limit != null && limitValue == null || params.interval != null && intervalValue == null
            || params.aggregationFunction != null && aggregationFuncValue == null) {
            return new EmptyIteration<>();
        }

        long begin = getLongValue(fromValue, 0L);
        long end = getLongValue(toValue, KvinTuple.TIME_MAX_VALUE);
        long limit = getLongValue(limitValue, 0);
        final long interval = getLongValue(intervalValue, 0);

        final String aggregationFunc;
        if (params.aggregationFunction != null) {
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
        Value indexValue = getVarValue(params.index, bs);

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
            int index;
            BindingSet next;

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                if (next != null) {
                    return true;
                }
                if (it != null) {
                    next = computeNext();
                }
                if (next == null && !properties.isEmpty()) {
                    // reset index
                    index = -1;

                    URI currentProperty = properties.remove();
                    if (currentProperty.isRelative()) {
                        currentPropertyIRI = vf.createIRI("r:" + currentProperty);
                    } else {
                        currentPropertyIRI = vf.createIRI(currentProperty.toString());
                    }

                    // System.out.println("item=" + item + " property=" + currentProperty + " bindings=" + bs);

                    // create iterator with values for current property
                    it = kvin.fetch(item, currentProperty, context[0], endFinal, beginFinal, limitFinal, interval, aggregationFunc);
                    next = computeNext();
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
                if (it == null) {
                    return null;
                }
                while (it.hasNext()) {
                    KvinTuple tuple = it.next();
                    // adds a zero-based index to each returned tuple
                    index++;

                    // filters any tuple that does not match the requested seqNr, if any
                    if (seqNrValueInt != null && seqNrValueInt.intValue() != tuple.seqNr) {
                        continue;
                    }

                    // filters any tuple that does not match the requested index, if any
                    if (indexValue != null && (!indexValue.isLiteral() || ((Literal) indexValue).intValue() != index)) {
                        continue;
                    }

                    QueryBindingSet newBs = new QueryBindingSet(bs);
                    if (!objectVar.isConstant() && !bs.hasBinding(objectVar.getName())) {
                        Value objectValue = new BNodeWithValue(tuple);
                        newBs.addBinding(objectVar.getName(), objectValue);
                    }
                    if (!predVar.isConstant()) {
                        newBs.addBinding(predVar.getName(), currentPropertyIRI);
                    }
                    if (time != null && !time.isConstant() && !bs.hasBinding(time.getName())) {
                        newBs.addBinding(time.getName(), toRdfValue(tuple.time, vf));
                    }
                    if (contextVar != null && !contextVar.isConstant()) {
                        newBs.addBinding(contextVar.getName(), contextValue[0]);
                    }
                    if (seqNr != null && seqNrValue == null) {
                        newBs.addBinding(seqNr.getName(), toRdfValue(tuple.seqNr, vf));
                    }
                    if (params.index != null && indexValue == null) {
                        newBs.addBinding(params.index.getName(), toRdfValue(index, vf));
                    }

                    return newBs;
                }
                it.close();
                it = null;
                return null;
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

    public static boolean containsFetch(TupleExpr t) {
        TupleExpr n = t;
        ArrayDeque queue = null;
        do {
            if (n instanceof KvinFetch) {
                return true;
            }

            if (n instanceof Projection && ((Projection) n).isSubquery() || n instanceof Service) {
                return false;
            }

            List<TupleExpr> children = TupleExprs.getChildren(n);
            if (!children.isEmpty()) {
                if (queue == null) {
                    queue = new ArrayDeque();
                }
                queue.addAll(children);
            }
            n = queue != null ? (TupleExpr) queue.poll() : null;
        } while (n != null);
        return false;
    }
}
