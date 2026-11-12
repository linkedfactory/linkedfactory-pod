package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class FtsSail extends NotifyingSailWrapper {
	private final FtsIndexUpdateStrategy updateStrategy;
	private final Path outboxDirectory;
	private final String storeIdentity;
	private final FtsFederatedServiceConfig federatedServiceConfig;
	private final Set<String> excludedModels;
	private final FtsIndexUpdateCoordinatorFactory coordinatorFactory;
	private volatile FtsIndexUpdateCoordinator updateCoordinator;
	private volatile boolean shuttingDown;

	public FtsSail(FtsIndexUpdateStrategy updateStrategy, Path outboxDirectory, String storeIdentity,
			FtsFederatedServiceConfig federatedServiceConfig, Set<String> excludedModels) {
		this(updateStrategy, outboxDirectory, storeIdentity, federatedServiceConfig, excludedModels,
				new FileFtsIndexUpdateCoordinatorFactory());
	}

	public FtsSail(FtsIndexUpdateStrategy updateStrategy, Path outboxDirectory, String storeIdentity,
			FtsFederatedServiceConfig federatedServiceConfig, Set<String> excludedModels,
			FtsIndexUpdateCoordinatorFactory coordinatorFactory) {
		if (updateStrategy == null) {
			throw new NullPointerException("FTS update strategy is required");
		}
		if (coordinatorFactory == null) {
			throw new NullPointerException("FTS update coordinator factory is required");
		}
		this.updateStrategy = updateStrategy;
		this.coordinatorFactory = coordinatorFactory;
		this.outboxDirectory = outboxDirectory;
		this.storeIdentity = storeIdentity;
		this.federatedServiceConfig = federatedServiceConfig == null
				? FtsFederatedServiceConfig.defaults()
				: federatedServiceConfig;
		this.excludedModels = excludedModels == null || excludedModels.isEmpty()
				? Collections.emptySet()
				: Collections.unmodifiableSet(new LinkedHashSet<>(excludedModels));
	}

	public FtsFederatedServiceConfig getFederatedServiceConfig() {
		return federatedServiceConfig;
	}

	public Set<String> getExcludedModels() {
		return excludedModels;
	}

	public FtsOutboxStatus getOutboxStatus() throws SailException {
		FtsIndexUpdateCoordinator coordinator = updateCoordinator;
		if (coordinator == null) {
			throw new SailException("FTS update coordinator is not initialized");
		}
		try {
			return coordinator.status();
		} catch (Exception e) {
			throw new SailException("Unable to inspect FTS outbox status", e);
		}
	}

	@Override
	public synchronized void init() throws SailException {
		super.init();
		shuttingDown = false;
		try {
			updateCoordinator = coordinatorFactory.create(
					outboxDirectory, storeIdentity, updateStrategy, this::removeJournalMarker);
			recoverFtsTransactions();
		} catch (SailException e) {
			closeCoordinator(e);
			throw e;
		} catch (IOException e) {
			closeCoordinator(e);
			throw new SailException("Unable to initialize the FTS outbox", e);
		}
	}

	private void closeCoordinator(Exception failure) {
		FtsIndexUpdateCoordinator coordinator = updateCoordinator;
		updateCoordinator = null;
		if (coordinator != null) {
			try {
				coordinator.close();
			} catch (Exception closeError) {
				failure.addSuppressed(closeError);
			}
			try {
				coordinator.awaitTermination();
			} catch (Exception terminationError) {
				failure.addSuppressed(terminationError);
			}
		}
	}

	public synchronized void recoverFtsTransactions() throws SailException {
		if (shuttingDown) {
			throw new SailException("FTS Sail is shutting down");
		}
		FtsIndexUpdateCoordinator coordinator = updateCoordinator;
		if (coordinator == null) {
			throw new SailException("FTS update coordinator is not initialized");
		}
		try {
			coordinator.recover(this::readCommittedTransactionIds);
		} catch (Exception e) {
			throw new SailException("Unable to recover committed FTS transactions", e);
		}
	}

	private Set<String> readCommittedTransactionIds() throws SailException {
		Set<String> committedTransactionIds = new LinkedHashSet<>();
		try (SailConnection connection = getBaseSail().getConnection()) {
			try (var statements = connection.getStatements(null, FtsSailConnection.JOURNAL_PREDICATE, null, false,
					FtsSailConnection.JOURNAL_CONTEXT)) {
				while (statements.hasNext()) {
					Statement statement = statements.next();
					if (!(statement.getObject() instanceof Literal)) {
						throw new SailException("Invalid FTS transaction journal entry: expected a literal ID");
					}
					String transactionId = ((Literal) statement.getObject()).getLabel();
					if (!statement.getSubject().equals(SimpleValueFactory.getInstance()
							.createIRI("urn:linkedfactory:fts:transaction:" + transactionId))) {
						throw new SailException("Invalid FTS transaction journal entry for " + transactionId);
					}
					committedTransactionIds.add(transactionId);
				}
			}
		} catch (Exception e) {
			throw new SailException("Unable to read FTS transaction journal", e);
		}
		return Collections.unmodifiableSet(committedTransactionIds);
	}

	private void removeJournalMarker(String transactionId) throws SailException {
		try (SailConnection connection = getBaseSail().getConnection()) {
			connection.begin();
			connection.removeStatements(
					SimpleValueFactory.getInstance().createIRI("urn:linkedfactory:fts:transaction:" + transactionId),
					FtsSailConnection.JOURNAL_PREDICATE,
					SimpleValueFactory.getInstance().createLiteral(transactionId),
					FtsSailConnection.JOURNAL_CONTEXT);
			connection.commit();
		} catch (Exception e) {
			throw new SailException("Unable to remove completed FTS transaction marker " + transactionId, e);
		}
	}

	@Override
	public synchronized void shutDown() throws SailException {
		Exception failure = null;
		boolean workerTerminated = true;
		FtsIndexUpdateCoordinator coordinator = updateCoordinator;
		shuttingDown = true;
		if (coordinator != null) {
			try {
				coordinator.close();
			} catch (Exception e) {
				failure = e;
			}
			try {
				coordinator.awaitTermination();
			} catch (Exception e) {
				workerTerminated = false;
				if (failure == null) {
					failure = e;
				} else {
					failure.addSuppressed(e);
				}
			}
		}
		if (workerTerminated) {
			updateCoordinator = null;
			try {
				super.shutDown();
			} catch (Exception e) {
				if (failure == null) {
					failure = e;
				} else {
					failure.addSuppressed(e);
				}
			}
		}
		if (failure != null) {
			throw new SailException("Unable to shut down FTS Sail", failure);
		}
	}

	@Override
	public synchronized NotifyingSailConnection getConnection() throws SailException {
		if (shuttingDown || updateCoordinator == null) {
			throw new SailException("FTS update coordinator is not initialized");
		}
		SailConnection wrappedConnection = super.getConnection();
		if (!(wrappedConnection instanceof NotifyingSailConnection)) {
			wrappedConnection.close();
			throw new SailException("Wrapped SailConnection must implement NotifyingSailConnection.");
		}
		return new FtsSailConnection((NotifyingSailConnection) wrappedConnection, updateCoordinator, excludedModels);
	}
}
