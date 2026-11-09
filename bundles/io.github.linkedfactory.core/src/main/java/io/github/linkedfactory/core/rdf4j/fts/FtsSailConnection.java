package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class FtsSailConnection extends NotifyingSailConnectionWrapper {
	private static final int DEFAULT_MAX_BUFFERED_STATEMENTS = 5000;
	static final IRI JOURNAL_CONTEXT = SimpleValueFactory.getInstance().createIRI("urn:linkedfactory:fts:journal");
	static final IRI JOURNAL_PREDICATE = SimpleValueFactory.getInstance()
			.createIRI("urn:linkedfactory:fts:committed-transaction");

	private final FtsIndexUpdateCoordinator updateCoordinator;
	private final FtsSailBuffer buffer;
	private final Set<String> excludedModels;
	private boolean rdfCommitOutcomeUnknown;
	private boolean transactionFailed;
	private boolean requiresRecovery;

	FtsSailConnection(NotifyingSailConnection wrappedConnection, FtsIndexUpdateCoordinator updateCoordinator,
			Set<String> excludedModels) {
		this(wrappedConnection, updateCoordinator, excludedModels, DEFAULT_MAX_BUFFERED_STATEMENTS);
	}

	FtsSailConnection(NotifyingSailConnection wrappedConnection, FtsIndexUpdateCoordinator updateCoordinator,
			Set<String> excludedModels, int maxBufferedStatements) {
		super(wrappedConnection);
		this.updateCoordinator = updateCoordinator;
		this.excludedModels = excludedModels == null || excludedModels.isEmpty()
				? Collections.emptySet()
				: Set.copyOf(excludedModels);
		this.buffer = new FtsSailBuffer(maxBufferedStatements);
	}
	
	/**
	 * Determines whether a statement should be indexed for full-text search.
	 * <p>
	 * A statement is considered indexable if its object is a literal and, when it has
	 * a context, that context is not in the excluded models set.
	 *
	 * @param statement the statement to check
	 * @return true if the statement should be indexed, false otherwise
	 */
	private boolean isIndexedStatement(Statement statement) {
		Value object = statement.getObject();
		if (!(object instanceof Literal)) {
			return false;
		}
		Resource context = statement.getContext();
		return context == null
				|| (!JOURNAL_CONTEXT.equals(context) && !excludedModels.contains(context.stringValue()));
	}

	@Override
	public CloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred, Value obj,
			boolean includeInferred, Resource... contexts) throws SailException {
		CloseableIteration<? extends Statement> statements = super.getStatements(
				subj, pred, obj, includeInferred, contexts);
		return new CloseableIteration<Statement>() {
			private Statement next;
			private boolean closed;

			@Override
			public boolean hasNext() {
				while (!closed && next == null && statements.hasNext()) {
					Statement candidate = statements.next();
					if (!JOURNAL_CONTEXT.equals(candidate.getContext())) {
						next = candidate;
					}
				}
				return next != null;
			}

			@Override
			public Statement next() {
				if (!hasNext()) {
					throw new java.util.NoSuchElementException();
				}
				Statement result = next;
				next = null;
				return result;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void close() {
				if (!closed) {
					closed = true;
					statements.close();
				}
			}
		};
	}

	@Override
	public CloseableIteration<? extends Resource> getContextIDs() throws SailException {
		CloseableIteration<? extends Resource> contexts = super.getContextIDs();
		return new CloseableIteration<Resource>() {
			private Resource next;
			private boolean closed;

			@Override
			public boolean hasNext() {
				while (!closed && next == null && contexts.hasNext()) {
					Resource candidate = contexts.next();
					if (!JOURNAL_CONTEXT.equals(candidate)) {
						next = candidate;
					}
				}
				return next != null;
			}

			@Override
			public Resource next() {
				if (!hasNext()) {
					throw new java.util.NoSuchElementException();
				}
				Resource result = next;
				next = null;
				return result;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void close() {
				if (!closed) {
					closed = true;
					contexts.close();
				}
			}
		};
	}

	@Override
	public boolean hasStatement(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts)
			throws SailException {
		try (CloseableIteration<? extends Statement> statements = getStatements(
				subj, pred, obj, includeInferred, contexts)) {
			return statements.hasNext();
		}
	}

	@Override
	public long size(Resource... contexts) throws SailException {
		long count = 0;
		try (CloseableIteration<? extends Statement> statements = getStatements(null, null, null, false, contexts)) {
			while (statements.hasNext()) {
				statements.next();
				count++;
			}
		}
		return count;
	}

	@Override
	public synchronized void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		ensureTransactionUsable();
		if (contexts != null) {
			for (Resource context : contexts) {
				if (JOURNAL_CONTEXT.equals(context)) {
					throw new SailException("The FTS transaction journal context is reserved.");
				}
			}
		}
		try {
			super.addStatement(subj, pred, obj, contexts);
			if (contexts == null || contexts.length == 0) {
				recordAddedStatement(subj, pred, obj, null);
			} else {
				for (Resource context : contexts) {
					recordAddedStatement(subj, pred, obj, context);
				}
			}
		} catch (RuntimeException e) {
			transactionFailed = true;
			throw e;
		}
	}

	@Override
	public synchronized void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		ensureTransactionUsable();
		List<Statement> journalStatements = new ArrayList<>();
		try {
			collectStatementsForRemoval(subj, pred, obj, contexts, journalStatements);
			super.removeStatements(subj, pred, obj, contexts);
			restoreJournalStatements(journalStatements);
		} catch (RuntimeException e) {
			transactionFailed = true;
			throw e;
		}
	}

	@Override
	public synchronized void clear(Resource... contexts) throws SailException {
		ensureTransactionUsable();
		try {
			List<Statement> journalStatements = collectJournalStatements(null, null, null, contexts);
			super.clear(contexts);
			restoreJournalStatements(journalStatements);
			buffer.clear(contexts);
		} catch (RuntimeException e) {
			transactionFailed = true;
			throw e;
		}
	}

	@Override
	public void begin() throws SailException {
		ensureTransactionUsable();
		super.begin();
		buffer.reset();
		transactionFailed = false;
	}

	@Override
	public void begin(IsolationLevel level) throws SailException {
		ensureTransactionUsable();
		super.begin(level);
		buffer.reset();
		transactionFailed = false;
	}

	@Override
	public void startUpdate(UpdateContext updateContext) throws SailException {
		ensureTransactionUsable();
		super.startUpdate(updateContext);
	}

	@Override
	public synchronized void addStatement(UpdateContext updateContext, Resource subj, IRI pred, Value obj,
			Resource... contexts) throws SailException {
		ensureTransactionUsable();
		if (contexts != null) {
			for (Resource context : contexts) {
				if (JOURNAL_CONTEXT.equals(context)) {
					throw new SailException("The FTS transaction journal context is reserved.");
				}
			}
		}
		try {
			super.addStatement(updateContext, subj, pred, obj, contexts);
			if (contexts == null || contexts.length == 0) {
				recordAddedStatement(subj, pred, obj, null);
			} else {
				for (Resource context : contexts) {
					recordAddedStatement(subj, pred, obj, context);
				}
			}
		} catch (RuntimeException e) {
			transactionFailed = true;
			throw e;
		}
	}

	@Override
	public synchronized void removeStatement(UpdateContext updateContext, Resource subj, IRI pred, Value obj,
			Resource... contexts) throws SailException {
		ensureTransactionUsable();
		List<Statement> journalStatements = new ArrayList<>();
		try {
			collectStatementsForRemoval(subj, pred, obj, contexts, journalStatements);
			super.removeStatement(updateContext, subj, pred, obj, contexts);
			restoreJournalStatements(journalStatements);
		} catch (RuntimeException e) {
			transactionFailed = true;
			throw e;
		}
	}

	@Override
	public void endUpdate(UpdateContext updateContext) throws SailException {
		ensureTransactionUsable();
		super.endUpdate(updateContext);
	}

	@Override
	public void setNamespace(String prefix, String name) throws SailException {
		ensureTransactionUsable();
		super.setNamespace(prefix, name);
	}

	@Override
	public void removeNamespace(String prefix) throws SailException {
		ensureTransactionUsable();
		super.removeNamespace(prefix);
	}

	@Override
	public void clearNamespaces() throws SailException {
		ensureTransactionUsable();
		super.clearNamespaces();
	}

	@Override
	public void flush() throws SailException {
		ensureCommitOutcomeKnown();
		super.flush();
	}

	@Override
	public void prepare() throws SailException {
		ensureCommitOutcomeKnown();
		super.prepare();
	}

	@Override
	public void commit() throws SailException {
		ensureCommitOutcomeKnown();
		if (transactionFailed) {
			SailException failure = new SailException(
					"RDF transaction cannot commit after a failed mutation; it has been rolled back");
			try {
				rollback();
			} catch (SailException rollbackError) {
				failure.addSuppressed(rollbackError);
			}
			throw failure;
		}
		String transactionId = null;
		long sequence = -1;
		boolean rdfCommitAttempted = false;
		boolean rdfCommitted = false;
		boolean handlingFailure = false;
		FtsIndexUpdateCoordinator.CommitLock commitLock = null;
		Exception commitFailure = null;
		try {
			buffer.optimize();
			if (buffer.hasOperations()) {
				transactionId = UUID.randomUUID().toString();
				commitLock = updateCoordinator.acquireCommitLock();
				sequence = commitLock.nextSequence();
				super.addStatement(
						SimpleValueFactory.getInstance().createIRI("urn:linkedfactory:fts:transaction:" + transactionId),
						JOURNAL_PREDICATE,
						SimpleValueFactory.getInstance().createLiteral(transactionId),
						JOURNAL_CONTEXT);
			}

			if (transactionId != null) {
				updateCoordinator.prepare(transactionId, sequence, buffer.indexOperations());
			}
			rdfCommitAttempted = true;
			super.commit();
			rdfCommitted = true;
			if (transactionId != null) {
				updateCoordinator.activate(transactionId);
			}
		} catch (Exception e) {
			handlingFailure = true;
			commitFailure = e;
			// Distinguish an RDF commit success from an FTS activation failure
			if (rdfCommitted && transactionId != null) {
				requiresRecovery = true;
				throw new SailException(
						"RDF transaction committed successfully, but FTS activation failed. "
								+ "The FTS update remains recoverable.",
						e
				);
			}
			if (!rdfCommitted) {
				if (rdfCommitAttempted && transactionId != null) {
					rdfCommitOutcomeUnknown = true;
				} else {
					if (transactionId != null) {
						try {
							updateCoordinator.rollback(transactionId);
						} catch (Exception rollbackError) {
							e.addSuppressed(rollbackError);
						}
					}
					try {
						super.rollback();
					} catch (Exception rollbackError) {
						e.addSuppressed(rollbackError);
					}
				}
			}
			throw new SailException(e);
		} finally {
			if (commitLock != null) {
				try {
					commitLock.close();
				} catch (Exception lockError) {
					if (commitFailure == null) {
						commitFailure = lockError;
					} else {
						commitFailure.addSuppressed(lockError);
					}
				}
			}
			try {
				resetBuffer();
			} catch (RuntimeException resetError) {
				if (commitFailure == null) {
					commitFailure = resetError;
				} else {
					commitFailure.addSuppressed(resetError);
				}
			}
			if (commitFailure != null && !handlingFailure) {
				if (rdfCommitted) {
					throw new SailException(
							"RDF transaction committed successfully, but FTS transaction resource cleanup failed.",
							commitFailure);
				}
				throw new SailException("Unable to release FTS transaction resources", commitFailure);
			}
		}
	}

	void resetBuffer() {
		buffer.reset();
	}

	@Override
	public void rollback() throws SailException {
		ensureCommitOutcomeKnown();
		Exception failure = null;
		try {
			super.rollback();
		} catch (Exception e) {
			failure = e;
		}
		try {
			buffer.reset();
		} catch (Exception e) {
			if (failure == null) {
				failure = e;
			} else {
				failure.addSuppressed(e);
			}
		}
		if (failure != null) {
			throw new SailException(failure);
		}
		transactionFailed = false;
	}

	private void ensureConnectionUsable() throws SailException {
		if (!isOpen()) {
			throw new SailException("This FTS connection is closed");
		}
		if (requiresRecovery) {
			throw new SailException(
					"This connection cannot be reused because an FTS update "
							+ "requires recovery. Close the connection and "
							+ "allow the FTS coordinator to recover the update.");
		}
	}

	private void ensureTransactionUsable() throws SailException {
		ensureConnectionUsable();
		if (rdfCommitOutcomeUnknown) {
			throw new SailException("RDF commit outcome is unknown; close this connection and allow FTS recovery");
		}
		if (transactionFailed) {
			throw new SailException("RDF transaction has failed; roll it back before continuing");
		}
	}

	private void ensureCommitOutcomeKnown() throws SailException {
		ensureConnectionUsable();
		if (rdfCommitOutcomeUnknown) {
			throw new SailException("RDF commit outcome is unknown; close this connection and allow FTS recovery");
		}
	}

	@Override
	public void close() throws SailException {
		Exception failure = null;
		try {
			buffer.reset();
		} catch (Exception e) {
			failure = e;
		}
		try {
			super.close();
		} catch (Exception e) {
			if (failure == null) {
				failure = e;
			} else {
				failure.addSuppressed(e);
			}
		}
		if (failure != null) {
			throw new SailException(failure);
		}
	}

	private void recordAddedStatement(Resource subj, IRI pred, Value obj, Resource context) {
		Statement statement = SimpleValueFactory.getInstance().createStatement(subj, pred, obj, context);
		if (isIndexedStatement(statement)) {
			buffer.add(statement);
		}
	}

	private void collectStatementsForRemoval(Resource subj, IRI pred, Value obj, Resource[] contexts,
			List<Statement> journalStatements) throws SailException {
		try (var results = getWrappedConnection().getStatements(subj, pred, obj, false, contexts)) {
			while (results.hasNext()) {
				Statement statement = results.next();
				if (JOURNAL_CONTEXT.equals(statement.getContext())) {
					journalStatements.add(statement);
				} else if (isIndexedStatement(statement)) {
					buffer.remove(statement);
				}
			}
		}
	}

	private List<Statement> collectJournalStatements(Resource subj, IRI pred, Value obj, Resource... contexts)
			throws SailException {
		List<Statement> statements = new ArrayList<>();
		try (var results = getWrappedConnection().getStatements(subj, pred, obj, false, contexts)) {
			while (results.hasNext()) {
				Statement statement = results.next();
				if (JOURNAL_CONTEXT.equals(statement.getContext())) {
					statements.add(statement);
				}
			}
		}
		return statements;
	}

	private void restoreJournalStatements(List<Statement> statements) throws SailException {
		for (Statement statement : statements) {
			super.addStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(),
					statement.getContext());
		}
	}

	private static final class FtsSailBuffer {
		private final int maxBufferedStatements;
		private final ValueFactory vf = SimpleValueFactory.getInstance();
		private final List<Operation> operations = new ArrayList<>();

		private FtsSailBuffer(int maxBufferedStatements) {
			this.maxBufferedStatements = Math.max(maxBufferedStatements, 1);
		}

		void reset() {
			for (Operation operation : operations) {
				if (operation instanceof SpilledAddRemoveOperation) {
					((SpilledAddRemoveOperation) operation).delete();
				}
			}
			operations.clear();
		}

		List<Operation> operations() {
			return operations;
		}

		void add(Statement statement) {
			AddRemoveOperation op = ensureAddRemoveOperation();
			op.add(statement);
			if (op.statementCount() >= maxBufferedStatements) {
				spillTailOperation();
			}
		}

		void remove(Statement statement) {
			AddRemoveOperation op = ensureAddRemoveOperation();
			op.remove(statement);
			if (op.statementCount() >= maxBufferedStatements) {
				spillTailOperation();
			}
		}

		void clear(Resource... contexts) {
			if (contexts == null || contexts.length == 0) {
				reset();
				operations.add(new ClearOperation());
			} else {
				operations.add(new ClearContextOperation(contexts.clone()));
			}
		}

		void optimize() {
			for (int i = operations.size() - 1; i >= 0; i--) {
				if (operations.get(i) instanceof ClearOperation) {
					while (i > 0) {
						Operation discarded = operations.remove(i - 1);
						if (discarded instanceof SpilledAddRemoveOperation) {
							((SpilledAddRemoveOperation) discarded).delete();
						}
						i--;
					}
					break;
				}
			}
			for (Operation op : operations) {
				if (op instanceof AddRemoveOperation) {
					((AddRemoveOperation) op).optimize();
				}
			}
		}

		boolean hasOperations() {
			for (Operation operation : operations) {
				if (operation instanceof AddRemoveOperation && !((AddRemoveOperation) operation).isEmpty()) {
					return true;
				}
				if (operation instanceof SpilledAddRemoveOperation) {
					return true;
				}
				if (operation instanceof ClearOperation || operation instanceof ClearContextOperation) {
					return true;
				}
			}
			return false;
		}

		Iterable<FtsIndexOperation> indexOperations() {
			return () -> new Iterator<>() {
				private final Iterator<Operation> operationsIterator = operations.iterator();

				@Override
				public boolean hasNext() {
					return operationsIterator.hasNext();
				}

				@Override
				public FtsIndexOperation next() {
					Operation operation = operationsIterator.next();
					if (operation instanceof AddRemoveOperation addRemove) {
						return new FtsIndexOperation.Statements(addRemove.getAdded(), addRemove.getRemoved());
					}
					if (operation instanceof SpilledAddRemoveOperation spilled) {
						AddRemoveOperation addRemove = spilled.load();
						spilled.delete();
						return new FtsIndexOperation.Statements(addRemove.getAdded(), addRemove.getRemoved());
					}
					if (operation instanceof ClearOperation) {
						return new FtsIndexOperation.Clear();
					}
					if (operation instanceof ClearContextOperation clear) {
						return new FtsIndexOperation.ClearContexts(
								java.util.Arrays.stream(clear.getContexts()).filter(java.util.Objects::nonNull).toList());
					}
					throw new IllegalStateException("Unsupported operation type: " + operation.getClass().getName());
				}
			};
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
			if (tail instanceof SpilledAddRemoveOperation) {
				AddRemoveOperation op = new AddRemoveOperation();
				operations.add(op);
				return op;
			}
			AddRemoveOperation op = new AddRemoveOperation();
			operations.add(op);
			return op;
		}

		private void spillTailOperation() {
			if (operations.isEmpty()) {
				return;
			}
			int lastIndex = operations.size() - 1;
			Operation tail = operations.get(lastIndex);
			if (!(tail instanceof AddRemoveOperation)) {
				return;
			}
			AddRemoveOperation addRemove = (AddRemoveOperation) tail;
			addRemove.optimize();
			if (addRemove.isEmpty()) {
				operations.remove(lastIndex);
				return;
			}
			try {
				Path spillFile = Files.createTempFile("fts-sail-buffer-", ".bin");
				try (DataOutputStream out = new DataOutputStream(
						new BufferedOutputStream(Files.newOutputStream(spillFile)))) {
					writeStatements(out, addRemove.getAdded());
					writeStatements(out, addRemove.getRemoved());
				}
				operations.set(lastIndex, new SpilledAddRemoveOperation(spillFile));
			} catch (IOException e) {
				throw new RuntimeException("Unable to spill FTS buffer to disk", e);
			}
		}

		private void writeStatements(DataOutputStream out, Set<Statement> statements) throws IOException {
			out.writeInt(statements.size());
			for (Statement statement : statements) {
				writeResource(out, statement.getSubject());
				writeIRI(out, statement.getPredicate());
				writeValue(out, statement.getObject());
				writeResource(out, statement.getContext());
			}
		}

		private AddRemoveOperation readAddRemoveOperation(Path file) throws IOException {
			try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
				Set<Statement> added = readStatements(in);
				Set<Statement> removed = readStatements(in);
				AddRemoveOperation op = new AddRemoveOperation();
				for (Statement statement : added) {
					op.add(statement);
				}
				for (Statement statement : removed) {
					op.remove(statement);
				}
				return op;
			}
		}

		private Set<Statement> readStatements(DataInputStream in) throws IOException {
			int size = in.readInt();
			Set<Statement> statements = new LinkedHashSet<>(size);
			for (int i = 0; i < size; i++) {
				Resource subject = readResource(in);
				IRI predicate = readIRI(in);
				Value object = readValue(in);
				Resource context = readResource(in);
				statements.add(vf.createStatement(subject, predicate, object, context));
			}
			return statements;
		}

		private void writeResource(DataOutputStream out, Resource resource) throws IOException {
			if (resource == null) {
				out.writeByte(2);
				return;
			}
			if (resource instanceof IRI) {
				out.writeByte(0);
				writeString(out, resource.stringValue());
				return;
			}
			if (resource instanceof BNode) {
				out.writeByte(1);
				writeString(out, ((BNode) resource).getID());
				return;
			}
			throw new IOException("Unsupported resource type: " + resource.getClass().getName());
		}

		private Resource readResource(DataInputStream in) throws IOException {
			byte kind = in.readByte();
			if (kind == 2) {
				return null;
			}
			String value = readString(in);
			if (kind == 0) {
				return vf.createIRI(value);
			}
			if (kind == 1) {
				return vf.createBNode(value);
			}
			throw new IOException("Unsupported resource marker: " + kind);
		}

		private void writeIRI(DataOutputStream out, IRI iri) throws IOException {
			writeResource(out, iri);
		}

		private IRI readIRI(DataInputStream in) throws IOException {
			Resource resource = readResource(in);
			if (resource instanceof IRI) {
				return (IRI) resource;
			}
			throw new IOException("Expected IRI in spill file");
		}

		private void writeValue(DataOutputStream out, Value value) throws IOException {
			if (value instanceof IRI) {
				out.writeByte(0);
				writeString(out, value.stringValue());
				return;
			}
			if (value instanceof Literal) {
				Literal literal = (Literal) value;
				out.writeByte(1);
				writeString(out, literal.getLabel());
				writeString(out, literal.getLanguage().orElse(null));
				writeString(out, literal.getDatatype() == null ? null : literal.getDatatype().stringValue());
				return;
			}
			throw new IOException("Unsupported value type: " + value.getClass().getName());
		}

		private Value readValue(DataInputStream in) throws IOException {
			byte kind = in.readByte();
			String value = readString(in);
			if (kind == 0) {
				return vf.createIRI(value);
			}
			if (kind == 1) {
				String language = readString(in);
				String datatype = readString(in);
				if (language != null) {
					return datatype == null ? vf.createLiteral(value, language) : vf.createLiteral(value, language);
				}
				if (datatype != null) {
					return vf.createLiteral(value, vf.createIRI(datatype));
				}
				return vf.createLiteral(value);
			}
			throw new IOException("Unsupported value marker: " + kind);
		}

		private void writeString(DataOutputStream out, String value) throws IOException {
			if (value == null) {
				out.writeInt(-1);
				return;
			}
			byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
			out.writeInt(bytes.length);
			out.write(bytes);
		}

		private String readString(DataInputStream in) throws IOException {
			int length = in.readInt();
			if (length < 0) {
				return null;
			}
			byte[] bytes = new byte[length];
			in.readFully(bytes);
			return new String(bytes, StandardCharsets.UTF_8);
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

			boolean isEmpty() {
				return added.isEmpty() && removed.isEmpty();
			}

			int statementCount() {
				return added.size() + removed.size();
			}

			Set<Statement> getAdded() {
				return added;
			}

			Set<Statement> getRemoved() {
				return removed;
			}
		}

		final class SpilledAddRemoveOperation implements Operation {
			private final Path file;

			SpilledAddRemoveOperation(Path file) {
				this.file = file;
			}

			AddRemoveOperation load() {
				try {
					return readAddRemoveOperation(file);
				} catch (IOException e) {
					throw new RuntimeException("Unable to read spilled FTS buffer", e);
				}
			}

			void delete() {
				try {
					Files.deleteIfExists(file);
				} catch (IOException e) {
					throw new RuntimeException("Unable to delete spilled FTS buffer", e);
				}
			}
		}
	}
}
