package io.github.linkedfactory.service.rdf4j.query;

import io.github.linkedfactory.service.rdf4j.query.ParameterScanner.Parameters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class KvinFetchOptimizer extends AbstractQueryModelVisitor<RDF4JException> {

    ParameterScanner scanner;
    Map<Var, List<KvinPattern>> patternsByTime = new HashMap<>();

    public KvinFetchOptimizer(ParameterScanner scanner) {
        this.scanner = scanner;
    }

    /**
     * Extracts the parameters from the given <code>expr</code>.
     *
     * @param expr The expression with parameter statements.
     */
    public void process(TupleExpr expr) throws RDF4JException {
        expr.visit(this);
    }

    /*
     * service <kvin:> { <some:item> <some:prop> [ kvin:time ?t; kvin:value ?v ;
     * kvin:from 213123123; kvin:to 232131234] . }
     */

    @Override
    public void meet(Join node) throws RDF4JException {
        List<TupleExpr> joinArgs = new ArrayList<>();
        collectJoinArgs(node, joinArgs);
        for (TupleExpr expr : joinArgs) {
            if (expr instanceof StatementPattern) {
                StatementPattern stmt = (StatementPattern) expr;
                Parameters params = scanner.getParameters(stmt.getObjectVar());
                if (params != null && params.time != null) {
                    patternsByTime.computeIfAbsent(params.time, time -> new ArrayList<>()).add(new KvinPattern(stmt, params));
                }
            }
        }
        List<KvinFetch> kvinFetches = new ArrayList<>();
        patternsByTime.forEach((time, patterns) -> {
            KvinFetch timeJoin = new KvinFetch(patterns);
            kvinFetches.add(timeJoin);
            for (KvinPattern pattern : patterns) {
                pattern.getStatement().replaceWith(new SingletonSet());
            }
        });

        if (!kvinFetches.isEmpty()) {
            // Build new join hierarchy
            TupleExpr root = kvinFetches.get(0);
            for (int i = 1; i < kvinFetches.size(); i++) {
                root = new Join(kvinFetches.get(i), root);
            }

            Join newLeft = new Join();
            newLeft.setLeftArg(root);
            newLeft.setRightArg(node.getLeftArg());

            Join newNode = new Join();
            newNode.setLeftArg(newLeft);
            newNode.setRightArg(node.getRightArg());

            node.replaceWith(newNode);
        }
    }

    /**
     * Collect join arguments by descending the query tree (recursively).
     *
     * @param node
     * @param joinArgs
     */
    protected void collectJoinArgs(TupleExpr node, List<TupleExpr> joinArgs) {
        if (node instanceof Join) {
            collectJoinArgs(((Join) node).getLeftArg(), joinArgs);
            collectJoinArgs(((Join) node).getRightArg(), joinArgs);
        } else {
            joinArgs.add(node);
        }
    }
}
