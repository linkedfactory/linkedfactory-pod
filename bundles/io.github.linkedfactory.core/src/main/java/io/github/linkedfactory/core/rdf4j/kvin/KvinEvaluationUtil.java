package io.github.linkedfactory.core.rdf4j.kvin;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.rdf4j.common.BNodeWithValue;
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
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import java.util.NoSuchElementException;

import static io.github.linkedfactory.core.rdf4j.common.Conversions.getLongValue;
import static io.github.linkedfactory.core.rdf4j.common.Conversions.toRdfValue;
import static io.github.linkedfactory.core.rdf4j.kvin.KvinEvaluationStrategy.getVarValue;

public class KvinEvaluationUtil {

    private final Kvin kvin;

    public KvinEvaluationUtil(Kvin kvin) {
        this.kvin = kvin;
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
            aggregationFunc = aggregationFuncValue instanceof IRI ? ((IRI) aggregationFuncValue).getLocalName() :
                aggregationFuncValue.stringValue();
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

        final long beginFinal = begin, endFinal = end, limitFinal = limit;
        final URI predFinal = pred;
        final CloseableIteration<BindingSet, QueryEvaluationException> iteration = new AbstractCloseableIteration<BindingSet, QueryEvaluationException>() {
            IExtendedIterator<KvinTuple> it;
            URI currentProperty;
            IRI currentPropertyIRI;
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
                    it = kvin.fetch(item, predFinal, context[0], endFinal, beginFinal, limitFinal, interval, aggregationFunc);
                }
                if (it != null) {
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
                while (it.hasNext()) {
                    KvinTuple tuple = it.next();
                    // check if current property is changed
                    if (! tuple.property.equals(currentProperty)) {
                        // reset index
                        index = -1;

                        currentProperty = tuple.property;
                        if (currentProperty.isRelative()) {
                            currentPropertyIRI = vf.createIRI("r:" + currentProperty);
                        } else {
                            currentPropertyIRI = vf.createIRI(currentProperty.toString());
                        }
                        // filters tuples by property if endpoint does not support the filtering by property
                        skipProperty = predValue != null && !predValue.equals(currentPropertyIRI);
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
                    if (indexValue != null && (!indexValue.isLiteral() || ((Literal) indexValue).intValue() != index)) {
                        continue;
                    }

                    CompositeBindingSet newBs = new CompositeBindingSet(bs);
                    if (!objectVar.isConstant() && !bs.hasBinding(objectVar.getName())) {
                        Value objectValue = BNodeWithValue.create(tuple);
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
                close();
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
                    it = null;
                }
            }
        };
        return iteration;
    }
}
