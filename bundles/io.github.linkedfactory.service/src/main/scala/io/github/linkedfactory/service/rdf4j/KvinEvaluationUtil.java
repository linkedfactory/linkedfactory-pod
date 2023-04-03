package io.github.linkedfactory.service.rdf4j;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.service.rdf4j.KvinEvaluationStrategy.BNodeWithValue;
import io.github.linkedfactory.service.rdf4j.query.ParameterScanner.Parameters;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import java.util.LinkedList;

import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

import static io.github.linkedfactory.service.rdf4j.KvinEvaluationStrategy.*;

public class KvinEvaluationUtil {
    private final Kvin kvin;

    public KvinEvaluationUtil(Kvin kvin) {
        this.kvin = kvin;
    }

    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(ValueFactory vf,
        BindingSet bs, Parameters params, StatementPattern stmt) {
        net.enilink.komma.core.URI item = toKommaUri(getVarValue(stmt.getSubjectVar(), bs));

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
                    newBs.addBinding(time.getName(), toRdfValue(tuple.time, vf));
                }
                if (contextVar != null && !contextVar.isConstant()) {
                    newBs.addBinding(contextVar.getName(), contextValue[0]);
                }
                if (seqNr != null && seqNrValue == null) {
                    newBs.addBinding(seqNr.getName(), toRdfValue(tuple.seqNr, vf));
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
