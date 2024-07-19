package io.github.linkedfactory.core.rdf4j.kvin.query;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.github.linkedfactory.core.rdf4j.common.query.Fetch;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;

public class KvinFetch extends UnaryTupleOperator implements Fetch {
    final Parameters params;
    final Set<String> requiredBindings;

    public KvinFetch(StatementPattern stmt, Parameters params) {
        super(stmt);
        this.params = params;
        this.requiredBindings = computeRequiredBindings();
    }

    protected void addAdditionalBindingNames(Set<String> names, boolean assured) {
        if (params.time != null) {
            names.add(params.time.getName());
        }
        if (params.seqNr != null) {
            names.add(params.seqNr.getName());
        }
        if (params.index != null) {
            names.add(params.index.getName());
        }
        if (! assured) {
            if (params.from != null) {
                names.add(params.from.getName());
            }
            if (params.to != null) {
                names.add(params.to.getName());
            }
        }
    }

    @Override
    public Set<String> getBindingNames() {
        Set<String> bindingNames = new LinkedHashSet(16);
        bindingNames.addAll(getArg().getBindingNames());
        addAdditionalBindingNames(bindingNames, false);
        return bindingNames;
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        Set<String> assuredBindingNames = new LinkedHashSet(16);
        assuredBindingNames.add(getStatement().getPredicateVar().getName());
        assuredBindingNames.add(getStatement().getObjectVar().getName());
        addAdditionalBindingNames(assuredBindingNames, true);
        return assuredBindingNames;
    }

    public Parameters getParams() {
        return params;
    }

    public Set<String> getRequiredBindings() {
        return requiredBindings;
    }

    Set<String> computeRequiredBindings() {
        return Stream.of(getStatement().getSubjectVar(), params.from, params.to, params.interval, params.aggregationFunction)
            .filter(p -> p != null).map(p -> p.getName()).collect(
            Collectors.toSet());
    }

    @Override
    public <X extends Exception> void visit(QueryModelVisitor<X> queryModelVisitor) throws X {
        queryModelVisitor.meetOther(this);
    }

    public StatementPattern getStatement() {
        return (StatementPattern) getArg();
    }

    @Override
    public KvinFetch clone() {
        return (KvinFetch) super.clone();
    }
}
