package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FtsSailConnectionTest {
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();

	@Test
	public void commitPushesLiteralAndIriChanges() {
		NotifyingSailConnection wrapped = mock(NotifyingSailConnection.class);
		RecordingSearchService service = new RecordingSearchService();
		FtsSailConnection connection = new FtsSailConnection(wrapped, service);

		ArgumentCaptor<SailConnectionListener> listenerCaptor = ArgumentCaptor.forClass(SailConnectionListener.class);
		verify(wrapped).addConnectionListener(listenerCaptor.capture());
		SailConnectionListener listener = listenerCaptor.getValue();

		Statement addLiteral = vf.createStatement(
				vf.createIRI("urn:s1"),
				vf.createIRI("urn:p1"),
				vf.createLiteral("value")
		);
		Statement removeIri = vf.createStatement(
				vf.createIRI("urn:s2"),
				vf.createIRI("urn:p2"),
				vf.createIRI("urn:o2")
		);
		Statement ignored = vf.createStatement(
				vf.createIRI("urn:s3"),
				vf.createIRI("urn:p3"),
				vf.createBNode()
		);

		connection.begin();
		listener.statementAdded(addLiteral);
		listener.statementRemoved(removeIri);
		listener.statementAdded(ignored);
		connection.commit();

		assertEquals(1, service.addedStatements.size());
		assertEquals(1, service.removedStatements.size());
		assertTrue(service.addedStatements.contains(addLiteral));
		assertTrue(service.removedStatements.contains(removeIri));
	}

	@Test
	public void clearDropsPreviouslyBufferedOperations() {
		NotifyingSailConnection wrapped = mock(NotifyingSailConnection.class);
		RecordingSearchService service = new RecordingSearchService();
		FtsSailConnection connection = new FtsSailConnection(wrapped, service);

		ArgumentCaptor<SailConnectionListener> listenerCaptor = ArgumentCaptor.forClass(SailConnectionListener.class);
		verify(wrapped).addConnectionListener(listenerCaptor.capture());
		SailConnectionListener listener = listenerCaptor.getValue();

		connection.begin();
		Statement addLiteral = vf.createStatement(
				vf.createIRI("urn:s1"),
				vf.createIRI("urn:p1"),
				vf.createLiteral("value")
		);
		listener.statementAdded(addLiteral);
		connection.clear();
		connection.commit();

		assertEquals(1, service.clearCount);
		assertEquals(0, service.addRemoveCount);
		assertTrue(service.addedStatements.isEmpty());
		assertTrue(service.removedStatements.isEmpty());
	}

	@Test
	public void addAndRemoveSameStatementCancelOut() {
		NotifyingSailConnection wrapped = mock(NotifyingSailConnection.class);
		RecordingSearchService service = new RecordingSearchService();
		FtsSailConnection connection = new FtsSailConnection(wrapped, service);

		ArgumentCaptor<SailConnectionListener> listenerCaptor = ArgumentCaptor.forClass(SailConnectionListener.class);
		verify(wrapped).addConnectionListener(listenerCaptor.capture());
		SailConnectionListener listener = listenerCaptor.getValue();

		Statement stmt = vf.createStatement(
				vf.createIRI("urn:s1"),
				vf.createIRI("urn:p1"),
				vf.createLiteral("value")
		);

		connection.begin();
		listener.statementAdded(stmt);
		listener.statementRemoved(stmt);
		connection.commit();

		assertEquals(0, service.addRemoveCount);
		assertTrue(service.addedStatements.isEmpty());
		assertTrue(service.removedStatements.isEmpty());
	}

	@Test
	public void rollbackDiscardsPendingChanges() {
		NotifyingSailConnection wrapped = mock(NotifyingSailConnection.class);
		RecordingSearchService service = new RecordingSearchService();
		FtsSailConnection connection = new FtsSailConnection(wrapped, service);

		ArgumentCaptor<SailConnectionListener> listenerCaptor = ArgumentCaptor.forClass(SailConnectionListener.class);
		verify(wrapped).addConnectionListener(listenerCaptor.capture());
		SailConnectionListener listener = listenerCaptor.getValue();

		connection.begin();
		listener.statementAdded(vf.createStatement(vf.createIRI("urn:s1"), vf.createIRI("urn:p1"), vf.createLiteral("x")));
		connection.rollback();

		assertEquals(0, service.commitCount);
		assertEquals(1, service.rollbackCount);
		assertTrue(service.addedStatements.isEmpty());
	}

	@Test
	public void clearAddsClearContextOperation() {
		NotifyingSailConnection wrapped = mock(NotifyingSailConnection.class);
		RecordingSearchService service = new RecordingSearchService();
		FtsSailConnection connection = new FtsSailConnection(wrapped, service);

		IRI ctx = vf.createIRI("urn:ctx");
		connection.begin();
		connection.clear(ctx);
		connection.commit();

		assertEquals(1, service.clearedContexts.size());
		assertEquals(ctx, service.clearedContexts.get(0)[0]);
	}

	private static final class RecordingSearchService implements FtsSearchService {
		private final Set<Statement> addedStatements = new LinkedHashSet<>();
		private final Set<Statement> removedStatements = new LinkedHashSet<>();
		private final List<org.eclipse.rdf4j.model.Resource[]> clearedContexts = new ArrayList<>();
		private int addRemoveCount;
		private int clearCount;
		private int commitCount;
		private int rollbackCount;

		@Override
		public void addRemoveStatements(Set<Statement> added, Set<Statement> removed) {
			addRemoveCount++;
			addedStatements.addAll(added);
			removedStatements.addAll(removed);
		}

		@Override
		public void clearContexts(org.eclipse.rdf4j.model.Resource... contexts) {
			clearCount++;
			clearedContexts.add(contexts);
		}

		@Override
		public void clear() {
			clearCount++;
		}

		@Override
		public void commit() {
			commitCount++;
		}

		@Override
		public void rollback() {
			rollbackCount++;
		}
	}
}
