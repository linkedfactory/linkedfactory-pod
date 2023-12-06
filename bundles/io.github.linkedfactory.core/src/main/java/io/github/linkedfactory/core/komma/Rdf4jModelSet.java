package io.github.linkedfactory.core.komma;

import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.composition.annotations.Iri;
import net.enilink.komma.core.*;
import net.enilink.komma.model.MODELS;
import net.enilink.komma.model.rdf4j.MemoryModelSetSupport;
import net.enilink.komma.rdf4j.RDF4JValueConverter;
import org.apache.http.auth.AUTH;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.*;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;

import java.io.File;
import java.util.*;

@Iri(MODELS.NAMESPACE + "Rdf4jModelSet")
public abstract class Rdf4jModelSet extends MemoryModelSetSupport {
	@Iri(MODELS.NAMESPACE + "repository")
	public abstract IReference getRepository();

	public Repository createRepository() throws RepositoryException {
		IEntityManager manager = ((IEntity) getBehaviourDelegate()).getEntityManager();
		RDF4JValueConverter converter = new RDF4JValueConverter(SimpleValueFactory.getInstance());
		Model configModel = new LinkedHashModel();
		try (IExtendedIterator<IStatement> stmts = manager
				.createQuery("construct { ?s ?p ?o } where { ?repository (!<:>)* ?s . ?s ?p ?o }")
				.setParameter("repository", getRepository())
				.evaluateRestricted(IStatement.class)) {
			stmts.forEach(stmt -> {
				configModel.add(converter.toRdf4j(stmt));
			});
		}
		Set<String> repositoryIDs = RepositoryConfigUtil.getRepositoryIDs(configModel);
		if (repositoryIDs.isEmpty()) {
			throw new KommaException("No repository ID in configuration: " + getRepository());
		}
		if (repositoryIDs.size() != 1) {
			throw new KommaException("Multiple repository IDs in configuration: " + getRepository());
		}
		RepositoryConfig repoConfig = RepositoryConfigUtil.getRepositoryConfig(configModel,
				repositoryIDs.iterator().next());
		RepositoryImplConfig implConfig = repoConfig.getRepositoryImplConfig();
		if (implConfig == null) {
			throw new KommaException("No implementation config in configuration: " + getRepository());
		}
		RepositoryFactory factory = RepositoryRegistry.getInstance().get(implConfig.getType()).orElseThrow(() -> {
			return new RepositoryConfigException("Unsupported repository type: " + implConfig.getType());
		});
		Repository repository = factory.getRepository(implConfig);
		repository.init();
		addBasicKnowledge(repository);
		return repository;
	}

	@Override
	public boolean isPersistent() {
		return true;
	}
}
