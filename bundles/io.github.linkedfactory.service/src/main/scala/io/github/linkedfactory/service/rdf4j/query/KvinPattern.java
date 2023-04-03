package io.github.linkedfactory.service.rdf4j.query;

import io.github.linkedfactory.service.rdf4j.query.ParameterScanner.Parameters;
import java.util.LinkedHashSet;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

public class KvinPattern extends AbstractQueryModelNode implements TupleExpr {
    final StatementPattern stmt;
    final ParameterScanner.Parameters params;

    public KvinPattern(StatementPattern stmt, Parameters params) {
        this.stmt = stmt;
        this.params = params;
    }

    protected void addAdditionalBindingNames(Set<String> names) {
        if (params.time != null) {
            names.add(params.time.getName());
        }
        if (params.seqNr != null) {
            names.add(params.seqNr.getName());
        }
    }

    @Override
    public Set<String> getBindingNames() {
        Set<String> bindingNames = new LinkedHashSet(16);
        bindingNames.addAll(stmt.getBindingNames());
        addAdditionalBindingNames(bindingNames);
        return bindingNames;
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        Set<String> assuredBindingNames = new LinkedHashSet(16);
        assuredBindingNames.addAll(stmt.getAssuredBindingNames());
        addAdditionalBindingNames(assuredBindingNames);
        return assuredBindingNames;
    }

    @Override
    public <X extends Exception> void visit(QueryModelVisitor<X> queryModelVisitor) throws X {
        queryModelVisitor.meetOther(this);
    }

    public StatementPattern getStatement() {
        return stmt;
    }

    @Override
    public KvinPattern clone() {
        return (KvinPattern) super.clone();
    }
}
