package io.github.linkedfactory.core.rdf4j.kvin.query;

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

    public static Parameters combine(Parameters params, Parameters defaultParams) {
        Parameters result = new Parameters();
        result.from = valueOrDefault(params.from, defaultParams.from);
        result.to = valueOrDefault(params.to, defaultParams.to);
        result.limit = valueOrDefault(params.limit, defaultParams.limit);
        result.interval = valueOrDefault(params.interval, defaultParams.interval);
        result.aggregationFunction = valueOrDefault(params.aggregationFunction, defaultParams.aggregationFunction);
        result.time = valueOrDefault(params.time, defaultParams.time);
        result.seqNr = valueOrDefault(params.seqNr, defaultParams.seqNr);
        result.index = valueOrDefault(params.index, defaultParams.index);
        return result;
    }

    private static <T> T valueOrDefault(T value, T defaultValue) {
        return value != null ? value : defaultValue;
    }

    @Override
    protected Parameters clone() {
        try {
            return (Parameters) super.clone();
        } catch (Exception e) {
            // should never happen
            throw new RuntimeException(e);
        }
    }
}
