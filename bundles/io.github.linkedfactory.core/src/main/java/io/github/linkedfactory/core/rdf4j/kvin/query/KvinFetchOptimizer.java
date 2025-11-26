package io.github.linkedfactory.core.rdf4j.kvin.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class KvinFetchOptimizer extends AbstractQueryModelVisitor<RDF4JException> {

    ParameterScanner scanner;

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

    @Override
    public void meet(Filter node) throws RDF4JException {
        TupleExpr arg = node.getArg();
        // TODO check if this condition is really required
        if (arg instanceof StatementPattern stmt) {
	        Parameters params = scanner.getParameters(stmt.getObjectVar());
            if (params != null) {
                node.setArg(new KvinFetch(stmt, scanner.getParameters(stmt)));
            }
        } else {
            node.visitChildren(this);
        }
    }

    /*
     * service <kvin:> { <some:item> <some:prop> [ kvin:time ?t; kvin:value ?v ;
     * kvin:from 213123123; kvin:to 232131234] . }
     */

    @Override
    public void meet(Join node) throws RDF4JException {
        List<KvinFetch> kvinFetches = new ArrayList<>();

        List<TupleExpr> joinArgs = new ArrayList<>();
        collectJoinArgs(node, joinArgs);
        for (Iterator<TupleExpr> it = joinArgs.iterator(); it.hasNext(); ) {
            TupleExpr expr = it.next();
            if (expr instanceof StatementPattern stmt) {
	            Parameters params = scanner.getParameters(stmt.getObjectVar());
                if (params != null) {
                    stmt.replaceWith(new SingletonSet());
                    kvinFetches.add(new KvinFetch(stmt, scanner.getParameters(stmt)));
                }
                // no need to further inspect this
                it.remove();
            }
        }

        if (!kvinFetches.isEmpty()) {
            // Build new join hierarchy
            int i = kvinFetches.size() - 1;
            TupleExpr root = kvinFetches.get(i--);
            while (i >= 0) {
                root = new Join(kvinFetches.get(i--), root);
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
