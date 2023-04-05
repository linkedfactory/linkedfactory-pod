package io.github.linkedfactory.service.rdf4j;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.service.rdf4j.query.KvinFetch;
import io.github.linkedfactory.service.rdf4j.query.KvinFetchEvaluationStep;
import io.github.linkedfactory.service.rdf4j.query.ParameterScanner;
import io.github.linkedfactory.service.rdf4j.query.Parameters;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.common.iteration.IterationWrapper;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleBNode;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext.Minimal;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.HashJoinIteration;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import static io.github.linkedfactory.service.rdf4j.KvinEvaluationUtil.*;

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
                    Value valueVarValue = getVarValue(valueVar, bs);

                    Value rdfValue;
                    if (tuple.value instanceof Record) {
                        // value is an event, create a blank node
                        rdfValue = valueVar.isConstant() ? valueVar.getValue() : new BNodeWithValue(tuple.value);
                    } else {
                        // convert value to literal
                        rdfValue = toRdfValue(tuple.value, vf);
                    }

                    if (valueVarValue == null) {
                        newBs.addBinding(valueVar.getName(), rdfValue);
                        return new SingletonIteration<>(newBs);
                    } else if (valueVarValue.equals(rdfValue)) {
                        return new SingletonIteration<>(newBs);
                    }
                    return new EmptyIteration<>();
                } else if (KVIN.TIME.equals(predValue)) {
                    Var timeVar = stmt.getObjectVar();
                    Value timeVarValue = getVarValue(timeVar, bs);
                    Value timeValue = toRdfValue(tuple.time, vf);
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
                    Value seqNrValue = toRdfValue(tuple.seqNr, vf);
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
                        newBs.addBinding(valueVar.getName(), toRdfValue(r.getValue(), vf));
                    }
                    return new SingletonIteration<>(newBs);
                }
            }
        } else {
            if (bs.hasBinding(stmt.getObjectVar().getName())) {
                // bindings where already fully computed via scanner.referencedBy
                return new SingletonIteration<>(bs);
            }

            if (subjectValue != null && subjectValue.isIRI()) {
                Parameters params = scanner.getParameters(stmt.getObjectVar());
                return new KvinEvaluationUtil(kvin).evaluate(vf, bs, params == null ? new Parameters() : params, stmt);
            }
        }
        return super.evaluate(stmt, bs);
    }

    @Override
    protected QueryEvaluationStep prepare(LeftJoin join, QueryEvaluationContext context) throws QueryEvaluationException {
        if (containsFetch(join.getLeftArg()) && (join.getRightArg() instanceof KvinFetch ||
            join.getRightArg() instanceof Join && ((Join) join.getRightArg()).getLeftArg() instanceof KvinFetch)) {
            return bindingSet -> new HashJoinIteration(KvinEvaluationStrategy.this, join.getLeftArg(), join.getRightArg(), bindingSet,
                true);
        }
        return super.prepare(join, context);
    }

    @Override
    protected QueryEvaluationStep prepare(Join join, QueryEvaluationContext context) throws QueryEvaluationException {
        return bindingSet -> {
            if (containsFetch(join.getLeftArg()) && (join.getRightArg() instanceof KvinFetch ||
                join.getRightArg() instanceof Join && ((Join) join.getRightArg()).getLeftArg() instanceof KvinFetch)) {
                return new HashJoinIteration(KvinEvaluationStrategy.this, join.getLeftArg(), join.getRightArg(), bindingSet, false);
            }
            return new KvinJoinIterator(KvinEvaluationStrategy.this, join, bindingSet);
        };
    }

    protected boolean containsFetch(TupleExpr t) {
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

    @Override
    public QueryEvaluationStep precompile(TupleExpr expr, QueryEvaluationContext context) {
        if (expr instanceof KvinFetch) {
            return new KvinFetchEvaluationStep(KvinEvaluationStrategy.this, (KvinFetch) expr);
        }
        return super.precompile(expr, context);
    }

    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings)
        throws QueryEvaluationException {
        if (expr instanceof KvinFetch) {
            QueryEvaluationContext context = new Minimal(this.dataset, this.tripleSource.getValueFactory());
            return precompile(expr, context).evaluate(bindings);
        }
        return super.evaluate(expr, bindings);
    }

    public Kvin getKvin() {
        return kvin;
    }

    public ParameterScanner getScanner() {
        return scanner;
    }

    public ValueFactory getValueFactory() {
        return vf;
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