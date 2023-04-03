package io.github.linkedfactory.service.rdf4j.query;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

public class KvinFetch extends AbstractQueryModelNode implements TupleExpr {

    final List<KvinPattern> itemPropertyPatterns;

    public KvinFetch(List<KvinPattern> itemPropertyPatterns) {
        this.itemPropertyPatterns = itemPropertyPatterns;
    }

    @Override
    public Set<String> getBindingNames() {
        Set<String> bindingNames = new LinkedHashSet(16);
        itemPropertyPatterns.forEach(p -> bindingNames.addAll(p.getBindingNames()));
        return bindingNames;
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        Set<String> assuredBindingNames = new LinkedHashSet(16);
        itemPropertyPatterns.forEach(p -> assuredBindingNames.addAll(p.getAssuredBindingNames()));
        return assuredBindingNames;
    }

    @Override
    public <X extends Exception> void visit(QueryModelVisitor<X> queryModelVisitor) throws X {
        queryModelVisitor.meetOther(this);
    }

    public boolean equals(Object other) {
        return other instanceof KvinFetch && super.equals(other);
    }

    @Override
    public KvinFetch clone() {
        return (KvinFetch) super.clone();
    }
}
