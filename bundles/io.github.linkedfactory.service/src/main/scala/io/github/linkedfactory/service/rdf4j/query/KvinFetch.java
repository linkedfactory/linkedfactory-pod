package io.github.linkedfactory.service.rdf4j.query;

import java.util.LinkedHashSet;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;

public class KvinFetch extends UnaryTupleOperator implements TupleExpr {
    final Parameters params;

    public KvinFetch(StatementPattern stmt, Parameters params) {
        super(stmt);
        this.params = params;
    }

    protected void addAdditionalBindingNames(Set<String> names) {
        if (params.time != null) {
            names.add(params.time.getName());
        }
        if (params.seqNr != null) {
            names.add(params.seqNr.getName());
        }
        if (params.from != null) {
            names.add(params.from.getName());
        }
        if (params.to != null) {
            names.add(params.to.getName());
        }
        if (params.index != null) {
            names.add(params.index.getName());
        }
    }

    @Override
    public Set<String> getBindingNames() {
        Set<String> bindingNames = new LinkedHashSet(16);
        bindingNames.addAll(getArg().getBindingNames());
        addAdditionalBindingNames(bindingNames);
        return bindingNames;
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        Set<String> assuredBindingNames = new LinkedHashSet(16);
        assuredBindingNames.addAll(getArg().getAssuredBindingNames());
        addAdditionalBindingNames(assuredBindingNames);
        return assuredBindingNames;
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
