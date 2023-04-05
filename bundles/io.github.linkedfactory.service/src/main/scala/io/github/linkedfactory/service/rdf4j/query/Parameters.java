package io.github.linkedfactory.service.rdf4j.query;

import org.eclipse.rdf4j.query.algebra.Var;

public class Parameters implements Cloneable {

    public Var from;
    public Var to;
    public Var limit;
    public Var interval;
    public Var aggregationFunction;
    public Var time;
    public Var seqNr;
    public Var index;

    @Override
    protected Parameters clone() throws CloneNotSupportedException {
        return (Parameters) super.clone();
    }
}
