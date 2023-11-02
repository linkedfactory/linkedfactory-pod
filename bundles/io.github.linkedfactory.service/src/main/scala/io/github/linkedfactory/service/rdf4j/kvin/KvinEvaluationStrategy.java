package io.github.linkedfactory.service.rdf4j.kvin;

import static io.github.linkedfactory.service.rdf4j.kvin.KvinEvaluationUtil.compareAndBind;
import static io.github.linkedfactory.service.rdf4j.kvin.KvinEvaluationUtil.findFirstFetch;
import static io.github.linkedfactory.service.rdf4j.kvin.KvinEvaluationUtil.toKommaUri;
import static io.github.linkedfactory.service.rdf4j.kvin.KvinEvaluationUtil.toRdfValue;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.service.rdf4j.common.query.CompositeBindingSet;
import io.github.linkedfactory.service.rdf4j.common.query.InnerJoinIterator;
import io.github.linkedfactory.service.rdf4j.kvin.query.KvinFetch;
import io.github.linkedfactory.service.rdf4j.kvin.query.KvinFetchEvaluationStep;
import io.github.linkedfactory.service.rdf4j.kvin.query.ParameterScanner;
import io.github.linkedfactory.service.rdf4j.kvin.query.Parameters;
import net.enilink.vocab.rdf.RDF;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.common.iteration.IterationWrapper;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleBNode;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
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
            // this case should be handled by correctly defining the evaluation order by reordering the SPARQL AST nodes
            return new EmptyIteration<>();
        }

        Object data = subjectValue instanceof BNodeWithValue ? ((BNodeWithValue) subjectValue).value : null;
        if (data instanceof KvinTuple) {
            KvinTuple tuple = (KvinTuple) data;
            Value predValue = getVarValue(stmt.getPredicateVar(), bs);
            if (predValue != null) {
                if (KVIN.VALUE.equals(predValue)) {
                    Var valueVar = stmt.getObjectVar();
                    Value rdfValue = toRdfValue(tuple.value, vf);
                    return compareAndBind(bs, valueVar, rdfValue);
                } else if (KVIN.TIME.equals(predValue)) {
                    Var timeVar = stmt.getObjectVar();
                    Value timeValue = toRdfValue(tuple.time, vf);
                    return compareAndBind(bs, timeVar, timeValue);
                } else if (KVIN.SEQNR.equals(predValue)) {
                    Var seqNrVar = stmt.getObjectVar();
                    Value seqNrValue = toRdfValue(tuple.seqNr, vf);
                    return compareAndBind(bs, seqNrVar, seqNrValue);
                }
            }
        } else if (data instanceof Record) {
            Value predValue = getVarValue(stmt.getPredicateVar(), bs);
            net.enilink.komma.core.URI predicate = toKommaUri(predValue);
            if (predicate != null) {
                Record r = ((Record) data).first(predicate);
                if (r != Record.NULL) {
                    Var objectVar = stmt.getObjectVar();
                    Value newValue = toRdfValue(r.getValue(), vf);
                    return compareAndBind(bs, objectVar, newValue);
                }
            } else {
                Iterator<Record> it = ((Record) data).iterator();
                Var variable = stmt.getObjectVar();
                return new AbstractCloseableIteration<>() {
                    @Override
                    public boolean hasNext() throws QueryEvaluationException {
                        return it.hasNext();
                    }

                    @Override
                    public BindingSet next() throws QueryEvaluationException {
                        Record r = it.next();
                        CompositeBindingSet newBs = new CompositeBindingSet(bs);
                        newBs.addBinding(stmt.getPredicateVar().getName(), toRdfValue(r.getProperty(), vf));
                        newBs.addBinding(variable.getName(), toRdfValue(r.getValue(), vf));
                        return newBs;
                    }

                    @Override
                    public void remove() throws QueryEvaluationException {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        } else if (data instanceof Object[] || data instanceof List<?>) {
            List<?> list = data instanceof Object[] ? Arrays.asList((Object[]) data) : (List<?>) data;
            Value predValue = getVarValue(stmt.getPredicateVar(), bs);
            if (predValue == null) {
                Iterator<?> it = list.iterator();
                Var variable = stmt.getObjectVar();
                return new AbstractCloseableIteration<>() {
                    int i = 0;

                    @Override
                    public boolean hasNext() throws QueryEvaluationException {
                        return it.hasNext();
                    }

                    @Override
                    public BindingSet next() throws QueryEvaluationException {
                        QueryBindingSet newBs = new QueryBindingSet(bs);
                        newBs.addBinding(variable.getName(), toRdfValue(vf.createIRI(RDF.NAMESPACE, "_" + (++i)), vf));
                        return newBs;
                    }

                    @Override
                    public void remove() throws QueryEvaluationException {
                        throw new UnsupportedOperationException();
                    }
                };
            } else if (predValue.isIRI() && RDF.NAMESPACE.equals(((IRI) predValue).getNamespace())) {
                String localName = ((IRI) predValue).getLocalName();
                if (localName.matches("_[0-9]+")) {
                    int index = Integer.parseInt(localName.substring(1));
                    if (index > 0 && index <= list.size()) {
                        return compareAndBind(bs, stmt.getObjectVar(), toRdfValue(list.get(index - 1), vf));
                    }
                }
            }
            return new EmptyIteration<>();
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
        return new EmptyIteration<>();
    }

    @Override
    protected QueryEvaluationStep prepare(LeftJoin join, QueryEvaluationContext context) throws QueryEvaluationException {
        if (useHashJoin(join.getLeftArg(), join.getRightArg())) {
            return bindingSet -> new HashJoinIteration(KvinEvaluationStrategy.this, join.getLeftArg(), join.getRightArg(), bindingSet, true);
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
            // strictly use lateral joins if left arg contains a KVIN fetch as right arg probably depends on the results
            KvinFetch fetch = findFirstFetch(join.getLeftArg());
            boolean lateral = fetch != null;
            // do not use lateral join if left fetch requires a binding from the right join argument
            if (lateral) {
                // switch join order if left depends on right
                Set<String> assured = join.getRightArg().getAssuredBindingNames();
                boolean leftDependsOnRight = fetch.getRequiredBindings().stream()
                    .anyMatch(name -> assured.contains(name));
                if (leftDependsOnRight) {
                    // swap left and right argument
                    return bindingSet -> new InnerJoinIterator(KvinEvaluationStrategy.this,
                        rightPrepared, leftPrepared, bindingSet, true
                    );
                }
            }
            return bindingSet -> new InnerJoinIterator(KvinEvaluationStrategy.this,
                leftPrepared, rightPrepared, bindingSet, lateral
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
                return ! rightFetch.getRequiredBindings().stream().anyMatch(required -> leftAssured.contains(required));
            }
        }
        return false;
    }

    protected QueryEvaluationStep prepare(StatementPattern node, QueryEvaluationContext context) throws QueryEvaluationException {
        return bindingSet -> evaluate(node, bindingSet);
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
            return uniqueIdPrefix + uniqueIdSuffix.incrementAndGet();
        }
    }
}