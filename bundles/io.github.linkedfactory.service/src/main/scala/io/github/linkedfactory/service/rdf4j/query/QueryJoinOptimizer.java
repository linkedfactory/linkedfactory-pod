/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package io.github.linkedfactory.service.rdf4j.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;

/**
 * A query optimizer that re-orders nested Joins.
 *
 * @author Arjohn Kampman
 * @author James Leigh
 */
public class QueryJoinOptimizer implements QueryOptimizer {

	/**
	 * Applies generally applicable optimizations: path expressions are sorted from more to less specific.
	 *
	 * @param tupleExpr
	 */
	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new JoinVisitor());
	}

	/**
	 * @deprecated This class is protected for historic reasons only, and will be made private in a future major
	 *             release.
	 */
	@Deprecated
	protected class JoinVisitor extends AbstractQueryModelVisitor<RuntimeException> {

		Set<String> boundVars = new HashSet<>();

		@Override
		public void meet(LeftJoin leftJoin) {
			leftJoin.getLeftArg().visit(this);

			Set<String> origBoundVars = boundVars;
			try {
				boundVars = new HashSet<>(boundVars);
				boundVars.addAll(leftJoin.getLeftArg().getBindingNames());

				leftJoin.getRightArg().visit(this);
			} finally {
				boundVars = origBoundVars;
			}
		}

		@Override
		public void meet(StatementPattern node) throws RuntimeException {
			super.meet(node);
		}

		private void optimizePriorityJoin(Set<String> origBoundVars, TupleExpr join) {

			Set<String> saveBoundVars = boundVars;
			try {
				boundVars = new HashSet<>(origBoundVars);
				join.visit(this);
			} finally {
				boundVars = saveBoundVars;
			}
		}

		@Override
		public void meet(Join node) {

			Set<String> origBoundVars = boundVars;
			try {
				boundVars = new HashSet<>(boundVars);

				// Recursively get the join arguments
				List<TupleExpr> joinArgs = getJoinArgs(node, new ArrayList<>());

				// Reorder the subselects and extensions to a more optimal sequence
				List<TupleExpr> priorityArgs = new ArrayList<>(joinArgs.size());

				// get all extensions (BIND clause)
				List<TupleExpr> orderedExtensions = getExtensions(joinArgs);
				joinArgs.removeAll(orderedExtensions);
				priorityArgs.addAll(orderedExtensions);

				// get all subselects and order them
				List<TupleExpr> orderedSubselects = reorderSubselects(getSubSelects(joinArgs));
				joinArgs.removeAll(orderedSubselects);
				priorityArgs.addAll(orderedSubselects);

				// Build new join hierarchy
				TupleExpr priorityJoins = null;
				if (priorityArgs.size() > 0) {
					priorityJoins = priorityArgs.get(0);
					for (int i = 1; i < priorityArgs.size(); i++) {
						priorityJoins = new Join(priorityJoins, priorityArgs.get(i));
					}
				}

				// get all KVIN fetches
				List<TupleExpr> fetches = getFetches(joinArgs);
				joinArgs.removeAll(fetches);
				TupleExpr fetchJoins = null;
				if (fetches.size() > 0) {
					fetchJoins = fetches.get(0);
					for (int i = 1; i < fetches.size(); i++) {
						fetchJoins = new Join(fetches.get(i), fetchJoins);
					}
				}

				if (priorityJoins == null) {
					priorityJoins = fetchJoins;
				} else if (fetchJoins != null) {
					priorityJoins = new Join(priorityJoins, fetchJoins);
				}

				if (joinArgs.size() > 0) {
					// Note: generated hierarchy is right-recursive to help the
					// IterativeEvaluationOptimizer to factor out the left-most join
					// argument
					int i = joinArgs.size() - 1;
					TupleExpr replacement = joinArgs.get(i);
					for (i--; i >= 0; i--) {
						replacement = new Join(joinArgs.get(i), replacement);
					}

					if (priorityJoins != null) {
						replacement = new Join(priorityJoins, replacement);
					}

					// Replace old join hierarchy
					node.replaceWith(replacement);

					// we optimize after the replacement call above in case the optimize call below
					// recurses back into this function and we need all the node's parent/child pointers
					// set up correctly for replacement to work on subsequent calls
					if (priorityJoins != null) {
						optimizePriorityJoin(origBoundVars, priorityJoins);
					}

				} else {
					// only subselect/priority joins involved in this query.
					node.replaceWith(priorityJoins);
				}
			} finally {
				boundVars = origBoundVars;
			}
		}

		protected <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
			if (tupleExpr instanceof Join) {
				Join join = (Join) tupleExpr;
				getJoinArgs(join.getLeftArg(), joinArgs);
				getJoinArgs(join.getRightArg(), joinArgs);
			} else {
				joinArgs.add(tupleExpr);
			}

			return joinArgs;
		}

		protected <M extends Map<Var, Integer>> M getVarFreqMap(List<Var> varList, M varFreqMap) {
			for (Var var : varList) {
				Integer freq = varFreqMap.get(var);
				freq = (freq == null) ? 1 : freq + 1;
				varFreqMap.put(var, freq);
			}
			return varFreqMap;
		}

		protected List<TupleExpr> getFetches(List<TupleExpr> expressions) {
			List<TupleExpr> fetches = new ArrayList<>();
			for (TupleExpr expr : expressions) {
				if (expr instanceof KvinFetch) {
					fetches.add(expr);
				}
			}
			return fetches;
		}

		protected List<TupleExpr> getExtensions(List<TupleExpr> expressions) {
			List<TupleExpr> extensions = new ArrayList<>();
			for (TupleExpr expr : expressions) {
				if (TupleExprs.containsExtension(expr)) {
					extensions.add(expr);
				}
			}
			return extensions;
		}

		protected List<TupleExpr> getSubSelects(List<TupleExpr> expressions) {
			List<TupleExpr> subselects = new ArrayList<>();

			for (TupleExpr expr : expressions) {
				if (TupleExprs.containsSubquery(expr)) {
					subselects.add(expr);
				}
			}
			return subselects;
		}

		/**
		 * Determines an optimal ordering of subselect join arguments, based on variable bindings. An ordering is
		 * considered optimal if for each consecutive element it holds that first of all its shared variables with all
		 * previous elements is maximized, and second, the union of all its variables with all previous elements is
		 * maximized.
		 * <p>
		 * Example: reordering
		 *
		 * <pre>
		 *   [f] [a b c] [e f] [a d] [b e]
		 * </pre>
		 * <p>
		 * should result in:
		 *
		 * <pre>
		 *   [a b c] [a d] [b e] [e f] [f]
		 * </pre>
		 *
		 * @param subselects the original ordering of expressions
		 * @return the optimized ordering of expressions
		 */
		protected List<TupleExpr> reorderSubselects(List<TupleExpr> subselects) {

			if (subselects.size() == 1) {
				return subselects;
			}

			List<TupleExpr> result = new ArrayList<>();
			if (subselects == null || subselects.isEmpty()) {
				return result;
			}

			// Step 1: determine size of join for each pair of arguments
			HashMap<Integer, List<TupleExpr[]>> joinSizes = new HashMap<>();

			int maxJoinSize = 0;
			for (int i = 0; i < subselects.size(); i++) {
				TupleExpr firstArg = subselects.get(i);
				for (int j = i + 1; j < subselects.size(); j++) {
					TupleExpr secondArg = subselects.get(j);

					Set<String> firstArgBindingNames = firstArg.getBindingNames();
					Set<String> secondArgBindingNames = secondArg.getBindingNames();
					int joinSize = getJoinSize(secondArgBindingNames, firstArgBindingNames);

					if (joinSize > maxJoinSize) {
						maxJoinSize = joinSize;
					}

					List<TupleExpr[]> l;

					if (joinSizes.containsKey(joinSize)) {
						l = joinSizes.get(joinSize);
					} else {
						l = new ArrayList<>();
					}
					TupleExpr[] tupleTuple = new TupleExpr[] { firstArg, secondArg };
					l.add(tupleTuple);
					joinSizes.put(joinSize, l);
				}
			}

			// Step 2: find the first two elements for the ordered list by
			// selecting the pair with first of all,
			// the highest join size, and second, the highest union size.

			TupleExpr[] maxUnionTupleTuple = null;
			int currentUnionSize = -1;

			// get a list of all argument pairs with the maximum join size
			List<TupleExpr[]> list = joinSizes.get(maxJoinSize);

			// select the pair that has the highest union size.
			for (TupleExpr[] tupleTuple : list) {
				Set<String> names = tupleTuple[0].getBindingNames();
				names.addAll(tupleTuple[1].getBindingNames());
				int unionSize = names.size();

				if (unionSize > currentUnionSize) {
					maxUnionTupleTuple = tupleTuple;
					currentUnionSize = unionSize;
				}
			}

			// add the pair to the result list.
			result.add(maxUnionTupleTuple[0]);
			result.add(maxUnionTupleTuple[1]);

			// Step 3: sort the rest of the list by selecting and adding an element
			// at a time.
			while (result.size() < subselects.size()) {
				result.add(getNextSubselect(result, subselects));
			}

			return result;
		}

		private TupleExpr getNextSubselect(List<TupleExpr> currentList, List<TupleExpr> joinArgs) {

			// determine union of names of all elements currently in the list: this
			// corresponds to the projection resulting from joining all these
			// elements.
			Set<String> currentListNames = new HashSet<>();
			for (TupleExpr expr : currentList) {
				currentListNames.addAll(expr.getBindingNames());
			}

			// select the next argument from the list, by checking that it has,
			// first, the highest join size with the current list, and second, the
			// highest union size.
			TupleExpr selected = null;
			int currentUnionSize = -1;
			int currentJoinSize = -1;
			for (TupleExpr candidate : joinArgs) {
				if (!currentList.contains(candidate)) {
					Set<String> names = candidate.getBindingNames();
					int joinSize = getJoinSize(currentListNames, names);

					Set<String> candidateBindingNames = candidate.getBindingNames();
					int unionSize = getUnionSize(currentListNames, candidateBindingNames);

					if (joinSize > currentJoinSize) {
						selected = candidate;
						currentJoinSize = joinSize;
						currentUnionSize = unionSize;
					} else if (joinSize == currentJoinSize) {
						if (unionSize > currentUnionSize) {
							selected = candidate;
							currentJoinSize = joinSize;
							currentUnionSize = unionSize;
						}
					}
				}
			}

			return selected;
		}
	}

    private static int getUnionSize(Set<String> currentListNames, Set<String> candidateBindingNames) {
        int count = 0;
        for (String n : currentListNames) {
            if (!candidateBindingNames.contains(n)) {
                count++;
            }
        }
        return candidateBindingNames.size() + count;
    }

	private static int getJoinSize(Set<String> currentListNames, Set<String> names) {
		int count = 0;
		for (String name : names) {
			if (currentListNames.contains(name)) {
				count++;
			}
		}
		return count;
	}
}