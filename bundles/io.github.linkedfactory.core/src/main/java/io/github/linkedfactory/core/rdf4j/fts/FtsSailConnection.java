package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class FtsSailConnection extends NotifyingSailConnectionWrapper {
	private final FtsSearchService searchService;
	private final FtsSailBuffer buffer = new FtsSailBuffer();

	protected final SailConnectionListener connectionListener = new SailConnectionListener() {
		@Override
		public void statementAdded(Statement statement) {
			if (isIndexedStatement(statement)) {
				buffer.add(statement);
			}
		}

		@Override
		public void statementRemoved(Statement statement) {
			if (isIndexedStatement(statement)) {
				buffer.remove(statement);
			}
		}
	};

	public FtsSailConnection(NotifyingSailConnection wrappedConnection, FtsSearchService searchService) {
		super(wrappedConnection);
		this.searchService = searchService == null ? FtsSearchService.NOOP : searchService;
		wrappedConnection.addConnectionListener(connectionListener);
	}

	private boolean isIndexedStatement(Statement statement) {
		Value object = statement.getObject();
		return object instanceof Literal || object instanceof IRI;
	}

	@Override
	public synchronized void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		super.addStatement(subj, pred, obj, contexts);
	}

	@Override
	public synchronized void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		super.removeStatements(subj, pred, obj, contexts);
	}

	@Override
	public synchronized void clear(Resource... contexts) throws SailException {
		NotifyingSailConnection wrappedConnection = (NotifyingSailConnection) getWrappedConnection();
		wrappedConnection.removeConnectionListener(connectionListener);
		try {
			super.clear(contexts);
			buffer.clear(contexts);
		} finally {
			wrappedConnection.addConnectionListener(connectionListener);
		}
	}

	@Override
	public void begin() throws SailException {
		super.begin();
		buffer.reset();
		try {
			searchService.begin();
		} catch (Exception e) {
			throw new SailException(e);
		}
	}

	@Override
	public void commit() throws SailException {
		super.commit();
		try {
			buffer.optimize();

			for (Iterator<FtsSailBuffer.Operation> it = buffer.operations().iterator(); it.hasNext();) {
				FtsSailBuffer.Operation op = it.next();
				if (op instanceof FtsSailBuffer.AddRemoveOperation) {
					FtsSailBuffer.AddRemoveOperation addRemove = (FtsSailBuffer.AddRemoveOperation) op;
					searchService.addRemoveStatements(addRemove.getAdded(), addRemove.getRemoved());
				} else if (op instanceof FtsSailBuffer.ClearContextOperation) {
					searchService.clearContexts(((FtsSailBuffer.ClearContextOperation) op).getContexts());
				} else if (op instanceof FtsSailBuffer.ClearOperation) {
					searchService.clear();
				} else {
					throw new SailException("Unsupported operation type: " + op.getClass().getName());
				}
				it.remove();
			}
			searchService.commit();
		} catch (Exception e) {
			try {
				searchService.rollback();
			} catch (Exception rollbackError) {
				e.addSuppressed(rollbackError);
			}
			throw new SailException(e);
		} finally {
			buffer.reset();
		}
	}

	@Override
	public void rollback() throws SailException {
		super.rollback();
		buffer.reset();
		try {
			searchService.rollback();
		} catch (Exception e) {
			throw new SailException(e);
		}
	}

	@Override
	public void close() throws SailException {
		try {
			((NotifyingSailConnection) getWrappedConnection()).removeConnectionListener(connectionListener);
		} finally {
			super.close();
		}
	}

	private static final class FtsSailBuffer {
		private final List<Operation> operations = new ArrayList<>();

		void reset() {
			operations.clear();
		}

		List<Operation> operations() {
			return operations;
		}

		void add(Statement statement) {
			AddRemoveOperation op = ensureAddRemoveOperation();
			op.add(statement);
		}

		void remove(Statement statement) {
			AddRemoveOperation op = ensureAddRemoveOperation();
			op.remove(statement);
		}

		void clear(Resource... contexts) {
			if (contexts == null || contexts.length == 0) {
				operations.add(new ClearOperation());
			} else {
				operations.add(new ClearContextOperation(contexts));
			}
		}

		void optimize() {
			for (Operation op : operations) {
				if (op instanceof AddRemoveOperation) {
					((AddRemoveOperation) op).optimize();
				}
			}
		}

		private AddRemoveOperation ensureAddRemoveOperation() {
			if (operations.isEmpty()) {
				AddRemoveOperation op = new AddRemoveOperation();
				operations.add(op);
				return op;
			}
			Operation tail = operations.get(operations.size() - 1);
			if (tail instanceof AddRemoveOperation) {
				return (AddRemoveOperation) tail;
			}
			AddRemoveOperation op = new AddRemoveOperation();
			operations.add(op);
			return op;
		}

		interface Operation {
		}

		static final class ClearOperation implements Operation {
		}

		static final class ClearContextOperation implements Operation {
			private final Resource[] contexts;

			ClearContextOperation(Resource... contexts) {
				this.contexts = contexts;
			}

			Resource[] getContexts() {
				return contexts;
			}
		}

		static final class AddRemoveOperation implements Operation {
			private final Set<Statement> added = new LinkedHashSet<>();
			private final Set<Statement> removed = new LinkedHashSet<>();

			void add(Statement statement) {
				added.add(statement);
			}

			void remove(Statement statement) {
				removed.add(statement);
			}

			void optimize() {
				Set<Statement> overlap = new LinkedHashSet<>(added);
				overlap.retainAll(removed);
				added.removeAll(overlap);
				removed.removeAll(overlap);
			}

			Set<Statement> getAdded() {
				return added;
			}

			Set<Statement> getRemoved() {
				return removed;
			}
		}
	}
}
