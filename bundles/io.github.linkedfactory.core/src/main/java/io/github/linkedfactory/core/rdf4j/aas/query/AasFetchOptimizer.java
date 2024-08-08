package io.github.linkedfactory.core.rdf4j.aas.query;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AasFetchOptimizer extends AbstractQueryModelVisitor<RDF4JException> {

    ParameterScanner scanner;

    public AasFetchOptimizer(ParameterScanner scanner) {
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

    @Override
    public void meet(Filter node) throws RDF4JException {
        TupleExpr arg = node.getArg();
        // TODO check if this condition is really required
        if (arg instanceof StatementPattern) {
            StatementPattern stmt = (StatementPattern) arg;
            Parameters params = scanner.getParameters(stmt.getObjectVar());
            if (params != null) {
                node.setArg(new AasFetch(stmt, scanner.getParameters(stmt)));
            }
        } else {
            node.visitChildren(this);
        }
    }

    /*
     * service <aas:> { <http://...> <aas:shells> ?shell . }
     */

    @Override
    public void meet(StatementPattern stmt) throws RDF4JException {
        Parameters params = scanner.getParameters(stmt);
        if (params != null) {
            stmt.replaceWith(new AasFetch(stmt.clone(), scanner.getParameters(stmt)));
        }
    }

    @Override
    public void meet(Join node) throws RDF4JException {
        List<AasFetch> fetches = new ArrayList<>();

        List<TupleExpr> joinArgs = new ArrayList<>();
        collectJoinArgs(node, joinArgs);
        for (Iterator<TupleExpr> it = joinArgs.iterator(); it.hasNext(); ) {
            TupleExpr expr = it.next();
            if (expr instanceof StatementPattern) {
                StatementPattern stmt = (StatementPattern) expr;
                Parameters params = scanner.getParameters(stmt);
                if (params != null) {
                    stmt.replaceWith(new SingletonSet());
                    fetches.add(new AasFetch(stmt, scanner.getParameters(stmt)));
                }
                // no need to further inspect this
                it.remove();
            }
        }

        if (!fetches.isEmpty()) {
            // Build new join hierarchy
            int i = fetches.size() - 1;
            TupleExpr root = fetches.get(i--);
            while (i >= 0) {
                root = new Join(fetches.get(i--), root);
            }

            Join newJoin = new Join();
            newJoin.setLeftArg(root);

            Join newRight = new Join();
            newRight.setLeftArg(node.getLeftArg());
            newRight.setRightArg(node.getRightArg());

            newJoin.setRightArg(newRight);

            node.replaceWith(newJoin);
        }

        // inspect further nodes
        for (TupleExpr expr : joinArgs) {
            expr.visit(this);
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
