package io.github.linkedfactory.service.rdf4j.query;

import static io.github.linkedfactory.service.rdf4j.KvinEvaluationStrategy.getVarValue;

import io.github.linkedfactory.service.rdf4j.KvinEvaluationStrategy;
import io.github.linkedfactory.service.rdf4j.KvinEvaluationUtil;
import io.github.linkedfactory.service.rdf4j.query.ParameterScanner.Parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

public class KvinFetchEvaluationStep implements QueryEvaluationStep {

    protected final KvinEvaluationStrategy strategy;
    protected final KvinFetch join;
    protected final KvinEvaluationUtil evalUtil;

    public KvinFetchEvaluationStep(KvinEvaluationStrategy strategy, KvinFetch join) {
        this.strategy = strategy;
        this.join = join;
        this.evalUtil = new KvinEvaluationUtil(strategy.getKvin());
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bs) {
        Set<String> seenVars = new HashSet<>();
        Map<Value, List<MapBindingSet>> existingBindings = new HashMap<>();
        Map<Value, List<MapBindingSet>> matchedBindings;

        int i = -1;
        for (KvinPattern pattern : join.itemPropertyPatterns) {
            i++;
            matchedBindings = new HashMap<>();

            Set<String> matchVars = pattern.getBindingNames().stream()
                .filter(name -> seenVars.contains(name))
                .collect(Collectors.toSet());

            StatementPattern stmt = pattern.getStatement();
            Value subjectValue = getVarValue(stmt.getSubjectVar(), bs);
            if (subjectValue != null && subjectValue.isIRI()) {
                Parameters params = pattern.params;
                try (CloseableIteration<BindingSet, QueryEvaluationException> it = evalUtil
                    .evaluate(strategy.getValueFactory(), bs, params, stmt)) {
                    while (it.hasNext()) {
                        BindingSet resultBs = it.next();
                        Value timeValue = getVarValue(params.time, resultBs);
                        List<MapBindingSet> existing = existingBindings.get(timeValue);
                        // first bindings do not have to match against themselves
                        if (i == 0) {
                            MapBindingSet mapBs = new MapBindingSet();
                            resultBs.forEach(b -> mapBs.setBinding(b));
                            matchedBindings.computeIfAbsent(timeValue, key -> new ArrayList<>()).add(mapBs);
                        } else if (existing != null) {
                            for (MapBindingSet existingBs : existing) {
                                if (compatible(matchVars, existingBs, resultBs)) {
                                    resultBs.forEach(b -> {
                                        // merge with existing bindings
                                        if (!existingBs.hasBinding(b.getName())) {
                                            existingBs.setBinding(b);
                                        }
                                    });
                                    matchedBindings.computeIfAbsent(timeValue, key -> new ArrayList<>()).add(existingBs);
                                }
                            }
                        }
                    }
                }
            } else {
                return new EmptyIteration<>();
            }
            seenVars.addAll(pattern.getBindingNames());
            existingBindings = matchedBindings;
        }
        Iterator<? extends BindingSet> it = existingBindings.values().stream().flatMap(l -> l.stream()).iterator();
        return new AbstractCloseableIteration<BindingSet, QueryEvaluationException>() {
            @Override
            public boolean hasNext() throws QueryEvaluationException {
                return it.hasNext();
            }

            @Override
            public BindingSet next() throws QueryEvaluationException {
                return it.next();
            }

            @Override
            public void remove() throws QueryEvaluationException {
            }
        };
    }

    private boolean compatible(Set<String> matchVars, BindingSet first, BindingSet second) {
        for (String name : matchVars) {
            Value firstValue = first.getValue(name);
            if (firstValue != null && !firstValue.equals(second.getValue(name))) {
                return false;
            }
        }
        return true;
    }
}
