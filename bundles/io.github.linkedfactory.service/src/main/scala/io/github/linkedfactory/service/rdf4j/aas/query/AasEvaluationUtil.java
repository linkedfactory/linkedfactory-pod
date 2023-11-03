package io.github.linkedfactory.service.rdf4j.aas.query;

import io.github.linkedfactory.service.rdf4j.aas.AAS;
import io.github.linkedfactory.service.rdf4j.aas.AasClient;
import io.github.linkedfactory.service.rdf4j.common.BNodeWithValue;
import io.github.linkedfactory.service.rdf4j.common.query.CompositeBindingSet;
import net.enilink.commons.iterator.IExtendedIterator;
import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy.getVarValue;

public class AasEvaluationUtil {
	private final AasClient client;

	public AasEvaluationUtil(AasClient client) {
		this.client = client;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			ValueFactory vf, BindingSet bs, Parameters params, StatementPattern stmt) {
		final Var predVar = stmt.getPredicateVar();
		final Var objectVar = stmt.getObjectVar();

		final Value subjValue = getVarValue(stmt.getSubjectVar(), bs);
		final Value predValue = getVarValue(predVar, bs);

		if (subjValue != null && AAS.SHELLS.equals(predValue)) {
			final CloseableIteration<BindingSet, QueryEvaluationException> iteration = new AbstractCloseableIteration<>() {
				IExtendedIterator<?> it;

				@Override
				public boolean hasNext() throws QueryEvaluationException {
					if (it == null && !isClosed()) {
						try {
							it = client.shells(subjValue.stringValue());
						} catch (URISyntaxException e) {
							throw new QueryEvaluationException(e);
						} catch (IOException e) {
							throw new QueryEvaluationException(e);
						}
					}
					return it != null && it.hasNext();
				}

				@Override
				public BindingSet next() throws QueryEvaluationException {
					Object value = it.next();
					CompositeBindingSet newBs = new CompositeBindingSet(bs);
					if (!objectVar.isConstant() && !bs.hasBinding(objectVar.getName())) {
						Value objectValue = BNodeWithValue.create(value);
						newBs.addBinding(objectVar.getName(), objectValue);
					}
					return newBs;
				}

				@Override
				public void remove() throws QueryEvaluationException {
					throw new UnsupportedOperationException();
				}

				@Override
				protected void handleClose() throws QueryEvaluationException {
					if (it != null) {
						it.close();
						it = null;
					}
					super.handleClose();
				}
			};
			return iteration;
		}
		return new EmptyIteration<>();
	}
}
