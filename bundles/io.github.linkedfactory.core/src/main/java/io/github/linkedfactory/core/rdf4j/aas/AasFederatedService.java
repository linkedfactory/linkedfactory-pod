package io.github.linkedfactory.core.rdf4j.aas;

import io.github.linkedfactory.core.rdf4j.aas.query.AasEvaluationStrategy;
import io.github.linkedfactory.core.rdf4j.aas.query.AasFetchOptimizer;
import io.github.linkedfactory.core.rdf4j.aas.query.ParameterScanner;
import io.github.linkedfactory.core.rdf4j.common.query.CompositeBindingSet;
import io.github.linkedfactory.core.rdf4j.common.query.QueryJoinOptimizer;
import io.github.linkedfactory.core.rdf4j.common.query.QueryModelPruner;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.common.iteration.*;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class AasFederatedService implements FederatedService {

    static final ValueFactory vf = SimpleValueFactory.getInstance();
    protected final Map<Service, Service> parsedServices = new WeakHashMap<>();
    protected boolean initialized = false;
    AasClient client;
    Supplier<ExecutorService> executorService;

    public AasFederatedService(String endpoint, Supplier<ExecutorService> executorService) {
        this(new AasClient(endpoint, vf), executorService);
    }

    public AasFederatedService(AasClient client, Supplier<ExecutorService> executorService) {
        this.client = client;
        this.executorService = executorService;
    }

    @Override
    public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {
        // ensures that original expression string is used
        service = parseExpression(service, baseUri);

        final CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluate(service,
                new SingletonIteration<>(bindings), baseUri);
        try {
            while (iter.hasNext()) {
                BindingSet bs = iter.next();
                String firstVar = service.getBindingNames().iterator().next();
                return QueryEvaluationUtil.getEffectiveBooleanValue(bs.getValue(firstVar));
            }
        } finally {
            iter.close();
        }
        return false;
    }

    Service parseExpression(Service service, String baseUri) {
        Service replacement;
        synchronized (parsedServices) {
            replacement = parsedServices.get(service);
        }
        if (replacement == null) {
            // instead of using the given tuple expr of service we parse the original expression string
            // to preserve the order of its patterns
            var parsed = new SPARQLParser()
                    .parseQuery(service.getSelectQueryString(Collections.emptySet()), baseUri).getTupleExpr();
            var newServiceExpr = ((Projection) ((QueryRoot) parsed).getArg()).getArg();
            // replace the service with a new one based on the original expression string
            replacement = new Service(service.getServiceRef().clone(), newServiceExpr, service.getServiceExpressionString(),
                    service.getPrefixDeclarations(), service.getBaseURI(), service.isSilent());
            synchronized (parsedServices) {
                parsedServices.put(service, replacement);
            }
        }
        return replacement.clone();
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service,
                                                                             CloseableIteration<BindingSet, QueryEvaluationException> bindings, String baseUri)
            throws QueryEvaluationException {
        if (!bindings.hasNext()) {
            return new EmptyIteration<>();
        }

        TupleExpr expr = service.getArg();
        final ParameterScanner scanner = new ParameterScanner();
        try {
            scanner.process(expr);
        } catch (RDF4JException e) {
            throw new QueryEvaluationException(e);
        }

        final AasFetchOptimizer fetchOptimizer = new AasFetchOptimizer(scanner);
        try {
            fetchOptimizer.process(service);
        } catch (RDF4JException e) {
            throw new QueryEvaluationException(e);
        }

        List<QueryOptimizer> optimizers = new ArrayList<>();
        optimizers.add(new QueryModelPruner());
        final QueryJoinOptimizer queryJoinOptimizer = new QueryJoinOptimizer();
        optimizers.add(queryJoinOptimizer);
        try {
            for (QueryOptimizer optimizer : optimizers) {
                optimizer.optimize(service, null, null);
            }
        } catch (RDF4JException e) {
            throw new QueryEvaluationException(e);
        }

        // for debugging purposes
        System.out.println(service);

        EvaluationStrategy strategy = new AasEvaluationStrategy(client, executorService, scanner, vf, null, null);

        List<CloseableIteration<BindingSet, QueryEvaluationException>> resultIters = new ArrayList<>();
        while (bindings.hasNext()) {
            BindingSet bs = bindings.next();
            resultIters.add(strategy.evaluate(service.getArg(), bs));
        }

        return resultIters.size() > 1 ? new DistinctIteration<>(new UnionIteration<>(resultIters)) : resultIters.get(0);
    }

    @Override
    public void initialize() throws QueryEvaluationException {
        initialized = true;
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service,
                                                                           final Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
        // ensures that original expression string is used
        service = parseExpression(service, baseUri);

        final CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluate(service,
                new SingletonIteration<>(bindings), baseUri);
        if (service.getBindingNames().equals(projectionVars)) {
            return iter;
        }

        return new CloseableIteration<>() {
            @Override
            public boolean hasNext() throws QueryEvaluationException {
                return iter.hasNext();
            }

            @Override
            public BindingSet next() throws QueryEvaluationException {
                CompositeBindingSet projected = new CompositeBindingSet(bindings);
                BindingSet result = iter.next();
                for (String var : projectionVars) {
                    Value v = result.getValue(var);
                    projected.addBinding(var, v);
                }
                return projected;
            }

            @Override
            public void remove() throws QueryEvaluationException {
                iter.remove();
            }

            @Override
            public void close() throws QueryEvaluationException {
                iter.close();
            }
        };
    }

    @Override
    public void shutdown() throws QueryEvaluationException {
        try {
            this.client.close();
        } catch (IOException e) {
            throw new QueryEvaluationException(e);
        }
    }
}
