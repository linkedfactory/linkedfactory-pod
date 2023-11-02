package io.github.linkedfactory.service.rdf4j.aas.query;

public class Parameters implements Cloneable {
    public static Parameters combine(Parameters params, Parameters defaultParams) {
        Parameters result = new Parameters();
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
