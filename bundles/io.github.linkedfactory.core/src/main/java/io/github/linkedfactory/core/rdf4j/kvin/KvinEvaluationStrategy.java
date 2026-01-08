package io.github.linkedfactory.core.rdf4j.kvin;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.util.JsonFormatWriter;
import io.github.linkedfactory.core.rdf4j.common.Conversions;
import io.github.linkedfactory.core.rdf4j.common.HasValue;
import io.github.linkedfactory.core.rdf4j.common.query.CompositeBindingSet;
import io.github.linkedfactory.core.rdf4j.common.query.InnerJoinIteratorEvaluationStep;
import io.github.linkedfactory.core.rdf4j.common.query.BatchQueryEvaluationStep;
import io.github.linkedfactory.core.rdf4j.kvin.query.*;
import net.enilink.komma.core.URI;
import net.enilink.vocab.rdf.RDF;
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
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext.Minimal;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.HashJoinIteration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.linkedfactory.core.rdf4j.common.query.Helpers.compareAndBind;
import static io.github.linkedfactory.core.rdf4j.common.query.Helpers.findFirstFetch;

public class KvinEvaluationStrategy extends DefaultEvaluationStrategy {
    static class InternalJsonFormatWriter extends JsonFormatWriter {
        InternalJsonFormatWriter(OutputStream outputStream) throws IOException {
            super(outputStream);
        }

        @Override
        protected void initialStartObject() {
            // do nothing
        }

        @Override
        protected void writeValue(Object value) throws IOException {
            super.writeValue(value);
        }

        @Override
        public void end() throws IOException {
            // do nothing
        }
    }

    final Kvin kvin;
    final ParameterScanner scanner;
    final ValueFactory vf;
    final Supplier<ExecutorService> executorService;

    public KvinEvaluationStrategy(Kvin kvin, Supplier<ExecutorService> executorService, ParameterScanner scanner, ValueFactory vf, Dataset dataset,
                                  FederatedServiceResolver serviceResolver, Map<Value, Object> valueToData) {
        super(new KvinTripleSource(vf), dataset, serviceResolver);
        this.kvin = kvin;
        this.scanner = scanner;
        this.vf = vf;
        this.executorService = executorService;
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(StatementPattern stmt, final BindingSet bs)
            throws QueryEvaluationException {
//        System.out.println("Stmt: " + stmt);

        final Var subjectVar = stmt.getSubjectVar();
        final Value subjectValue = DefaultEvaluationStrategy.getVarValue(subjectVar, bs);

        if (subjectValue == null) {
            // this happens for patterns like (:subject :property [ <kvin:value> ?someValue ])
            // where [ <kvin:value> ?someValue ] is evaluated first
            // this case should be handled by correctly defining the evaluation order by reordering the SPARQL AST nodes
            return new EmptyIteration<>();
        }

        Object data = subjectValue instanceof HasValue ? ((HasValue) subjectValue).getValue() : null;
        if (data instanceof KvinTuple) {
            KvinTuple tuple = (KvinTuple) data;
            Value predValue = DefaultEvaluationStrategy.getVarValue(stmt.getPredicateVar(), bs);
            if (predValue != null) {
                if (KVIN.VALUE.equals(predValue)) {
                    Var valueVar = stmt.getObjectVar();
                    Value rdfValue = Conversions.toRdfValue(tuple.value, vf);
                    return compareAndBind(bs, valueVar, rdfValue);
                } else if (KVIN.VALUE_JSON.equals(predValue)) {
                    Var valueVar = stmt.getObjectVar();
                    Value rdfValue;
                    if (tuple.value instanceof Record || tuple.value instanceof Object[] || tuple.value instanceof URI) {
                        var baos = new ByteArrayOutputStream();
                        try {
                            var writer = new InternalJsonFormatWriter(baos);
                            writer.writeValue(tuple.value);
                            writer.close();
                        } catch (Exception e) {
                            throw new QueryEvaluationException(e);
                        }
                        rdfValue = vf.createLiteral(baos.toString(StandardCharsets.UTF_8));
                    } else {
                        rdfValue = Conversions.toRdfValue(tuple.value, vf);
                    }
                    return compareAndBind(bs, valueVar, rdfValue);
                } else if (KVIN.TIME.equals(predValue)) {
                    Var timeVar = stmt.getObjectVar();
                    Value timeValue = Conversions.toRdfValue(tuple.time, vf);
                    return compareAndBind(bs, timeVar, timeValue);
                } else if (KVIN.SEQNR.equals(predValue)) {
                    Var seqNrVar = stmt.getObjectVar();
                    Value seqNrValue = Conversions.toRdfValue(tuple.seqNr, vf);
                    return compareAndBind(bs, seqNrVar, seqNrValue);
                }
            }
        } else if (data instanceof Record) {
            Value predValue = DefaultEvaluationStrategy.getVarValue(stmt.getPredicateVar(), bs);
            net.enilink.komma.core.URI predicate = KvinEvaluationUtil.toKommaUri(predValue);
            if (predicate != null) {
                Record r = ((Record) data).first(predicate);
                if (r != Record.NULL) {
                    Var objectVar = stmt.getObjectVar();
                    Value newValue = Conversions.toRdfValue(r.getValue(), vf);
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
                        newBs.addBinding(stmt.getPredicateVar().getName(), Conversions.toRdfValue(r.getProperty(), vf));
                        newBs.addBinding(variable.getName(), Conversions.toRdfValue(r.getValue(), vf));
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
            Var predVar = stmt.getPredicateVar();
            Value predValue = DefaultEvaluationStrategy.getVarValue(predVar, bs);
            if (predValue == null) {
                Iterator<?> it = list.iterator();
                Value objValue = DefaultEvaluationStrategy.getVarValue(stmt.getObjectVar(), bs);
                return new AbstractCloseableIteration<>() {
                    BindingSet next = null;
                    int i = 0;

                    @Override
                    public boolean hasNext() throws QueryEvaluationException {
                        while (next == null && it.hasNext()) {
                            Value elementValue = Conversions.toRdfValue(it.next(), vf);
                            if (objValue == null || objValue.equals(elementValue)) {
                                QueryBindingSet newBs = new QueryBindingSet(bs);
                                newBs.addBinding(predVar.getName(), vf.createIRI(RDF.NAMESPACE, "_" + (++i)));
                                newBs.addBinding(stmt.getObjectVar().getName(), elementValue);
                                next = newBs;
                            } else {
                                continue;
                            }
                        }
                        return next != null;
                    }

                    @Override
                    public BindingSet next() throws QueryEvaluationException {
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
            } else if (predValue.isIRI() && RDF.NAMESPACE.equals(((IRI) predValue).getNamespace())) {
                String localName = ((IRI) predValue).getLocalName();
                if (localName.matches("_[0-9]+")) {
                    int index = Integer.parseInt(localName.substring(1));
                    if (index > 0 && index <= list.size()) {
                        return compareAndBind(bs, stmt.getObjectVar(), Conversions.toRdfValue(list.get(index - 1), vf));
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
                return new KvinEvaluationUtil(kvin, executorService).evaluate(vf, bs, params == null ? new Parameters() : params, stmt, dataset);
            }
        }
        return new EmptyIteration<>();
    }

    @Override
    protected QueryEvaluationStep prepare(LeftJoin join, QueryEvaluationContext context) throws QueryEvaluationException {
        if (useHashJoin(join.getLeftArg(), join.getRightArg())) {
            QueryEvaluationStep leftPrepared = precompile(join.getLeftArg(), context);
            QueryEvaluationStep rightPrepared = precompile(join.getRightArg(), context);
            String[] joinAttributes = HashJoinIteration.hashJoinAttributeNames(join);
            return new BatchQueryEvaluationStep() {
                @Override
                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindingSet) {
                    return new HashJoinIteration(leftPrepared, rightPrepared, bindingSet, true, joinAttributes, context);
                }

                @Override
                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(List<BindingSet> bindingSets) {
                    return new HashJoinIteration(
                            BatchQueryEvaluationStep.evaluate(leftPrepared, bindingSets),
                            join.getLeftArg().getBindingNames(),
                            BatchQueryEvaluationStep.evaluate(rightPrepared, bindingSets),
                            join.getRightArg().getBindingNames(), true);
                }
            };
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
            Set<String> joinAttributesSet = Stream.of(joinAttributes).collect(Collectors.toSet());

            KvinFetch fetch = (KvinFetch) findFirstFetch(join.getLeftArg());
            Parameters params = fetch.getParams();
            final List<String> compareParams = new ArrayList<>();
            final List<Integer> compareSigns = new ArrayList<>();
            Var[] sortParams = new Var[]{params.index, params.time, params.seqNr};
            for (int i = 0; i < sortParams.length; i++) {
                var v = sortParams[i];
                if (v != null && joinAttributesSet.contains(v.getName())) {
                    compareParams.add(v.getName());
                    // index is ascending, time and seqNr are descending
                    compareSigns.add(i == 0 ? 1 : -1);
                }
            }

            if (!compareParams.isEmpty()) {
                return bindingSet -> MergeJoinIterator.getInstance(leftPrepared, rightPrepared, bindingSet,
                        new LongArrayComparator(compareSigns),
                        bs -> {
                            long[] values = new long[compareParams.size()];
                            int i = 0;
                            for (String name : compareParams) {
                                values[i++] = ((Literal) bs.getValue(name)).longValue();
                            }
                            return values;
                        }, context, executorService);
            } else {
                return new BatchQueryEvaluationStep() {
                    @Override
                    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindingSet) {
                        return new HashJoinIteration(leftPrepared, rightPrepared, bindingSet, false, joinAttributes, context);
                    }

                    @Override
                    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(List<BindingSet> bindingSets) {
                        return new HashJoinIteration(
                                BatchQueryEvaluationStep.evaluate(leftPrepared, bindingSets),
                                join.getLeftArg().getBindingNames(),
                                BatchQueryEvaluationStep.evaluate(rightPrepared, bindingSets),
                                join.getRightArg().getBindingNames(), false);
                    }
                };
            }
        } else {
            // strictly use lateral joins if left arg contains a KVIN fetch as right arg probably depends on the results
            KvinFetch fetch = (KvinFetch) findFirstFetch(join.getLeftArg());
            boolean lateral = fetch != null;
            // do not use lateral join if left fetch requires a binding from the right join argument
            if (lateral) {
                // switch join order if left depends on right
                Set<String> assured = join.getRightArg().getAssuredBindingNames();
                boolean leftDependsOnRight = fetch.getRequiredBindings().stream()
                        .anyMatch(name -> assured.contains(name));
                if (leftDependsOnRight) {
                    // swap left and right argument
                    return new InnerJoinIteratorEvaluationStep(KvinEvaluationStrategy.this, executorService,
                            rightPrepared, leftPrepared, true, true
                    );
                }
            }
            boolean async = findFirstFetch(join.getRightArg()) != null;
            return new InnerJoinIteratorEvaluationStep(KvinEvaluationStrategy.this, executorService,
                    leftPrepared, rightPrepared, lateral, async
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
                // in case of projections with aggregates we just use the projected binding names
                Set<String> leftAssured = leftArg instanceof Projection ? leftArg.getBindingNames() :
                        leftArg.getAssuredBindingNames();
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
        if (expr instanceof KvinFetch) {
            return new KvinFetchEvaluationStep(KvinEvaluationStrategy.this, (KvinFetch) expr, context);
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

    public Supplier<ExecutorService> getExecutorService() {
        return executorService;
    }

    static class LongArrayComparator implements Comparator<long[]> {
        final int[] signs;

        public LongArrayComparator(List<Integer> signs) {
            this.signs = new int[signs.size()];
            int i = 0;
            for (Integer sign : signs) {
                this.signs[i++] = sign;
            }
        }

        @Override
        public int compare(long[] a, long[] b) {
            for (int i = 0; i < a.length; i++) {
                if (i >= b.length) {
                    return 1;
                }
                int diff = signs[i] * Long.compare(a[i], b[i]);
                if (diff != 0) {
                    return diff;
                }
            }
            return 0;
        }
    }
}