/*
package io.github.linkedfactory.core.rdf4j.shacl;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

;import java.util.Set;

public  class ShaclFederatedService implements FederatedService {
    private final Repository repository;
    private boolean initialized = false;

    public ShaclFederationService(Repository repository) {
        this.repository = repository;
    }

    @Override
    public void initialize() {
        this.initialized = true;
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    private CloseableIteration<BindingSet, QueryEvaluationException> executeService(
            String sparql) throws QueryEvaluationException {

        try {
            RepositoryConnection conn = repository.getConnection();
            return conn.prepareTupleQuery(sparql).evaluate();
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> select(
            Service service,
            final Set<String> projectionVars,
            BindingSet bindings, String baseUri) throws QueryEvaluationException {

        // build SPARQL string out of service + bindings
        String sparql = buildServiceQuery(service, bindings, baseUri);
        return executeService(sparql);
    }

    @Override
    public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {

        String sparql = buildServiceQuery(service, bindings, baseUri);
        return executeService(sparql);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            Service service,
            CloseableIteration<BindingSet, QueryEvaluationException> bindings,
            String baseUri) throws QueryEvaluationException {

        String sparql = buildServiceQuery(service, bindings, baseUri);
        return executeService(sparql);
    }

    private String buildServiceQuery(Service service,
                                     BindingSet bindings,
                                     String baseUri) {

        // Simple fallback: serialize the algebra to SPARQL
        // (in LinkedFactory you may have a utility helper)
        return service.toString(); // naive; replace with proper SPARQL serialization
    }
}

 */
