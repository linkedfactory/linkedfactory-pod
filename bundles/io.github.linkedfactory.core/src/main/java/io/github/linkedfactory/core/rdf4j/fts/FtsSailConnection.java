package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class FtsSailConnection extends NotifyingSailConnectionWrapper {
	private static final int DEFAULT_MAX_BUFFERED_STATEMENTS = 5000;

	private final FtsSearchService searchService;
	private final FtsSailBuffer buffer;

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
		this(wrappedConnection, searchService, DEFAULT_MAX_BUFFERED_STATEMENTS);
	}

	FtsSailConnection(NotifyingSailConnection wrappedConnection, FtsSearchService searchService,
			int maxBufferedStatements) {
		super(wrappedConnection);
		this.searchService = searchService == null ? FtsSearchService.NOOP : searchService;
		this.buffer = new FtsSailBuffer(maxBufferedStatements);
		wrappedConnection.addConnectionListener(connectionListener);
	}

	/*
	  FIXME do we want to push literal properties and their linked IRIs to search.
      object instanceof IRI ensures statements like:
        ex:sensor1 ex:locatedIn ex:lineA .
        are captured for indexing as references/keywords (not full-text),
        so search can filter/join on linked resources.
      If only full-text over literal values is needed, remove that part and keep only object instanceof Literal.
	 */
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
					if (!addRemove.getAdded().isEmpty() || !addRemove.getRemoved().isEmpty()) {
						searchService.addRemoveStatements(addRemove.getAdded(), addRemove.getRemoved());
					}
				} else if (op instanceof FtsSailBuffer.SpilledAddRemoveOperation) {
					FtsSailBuffer.SpilledAddRemoveOperation spilled = (FtsSailBuffer.SpilledAddRemoveOperation) op;
					FtsSailBuffer.AddRemoveOperation addRemove = spilled.load();
					try {
						if (!addRemove.getAdded().isEmpty() || !addRemove.getRemoved().isEmpty()) {
							searchService.addRemoveStatements(addRemove.getAdded(), addRemove.getRemoved());
						}
					} finally {
						spilled.delete();
					}
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
			buffer.reset();
			super.close();
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
			reset();
			if (contexts == null || contexts.length == 0) {
				operations.add(new ClearOperation());
			} else {
				operations.add(new ClearContextOperation(contexts.clone()));
			}
		}

		void optimize() {
			for (int i = operations.size() - 1; i >= 0; i--) {
				if (operations.get(i) instanceof ClearOperation) {
					while (i > 0) {
						operations.remove(i - 1);
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
