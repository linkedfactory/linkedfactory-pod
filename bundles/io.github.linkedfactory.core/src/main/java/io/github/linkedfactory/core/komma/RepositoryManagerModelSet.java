package io.github.linkedfactory.core.komma;

import net.enilink.composition.annotations.Iri;
import net.enilink.komma.core.EntityVar;
import net.enilink.komma.core.IReference;
import net.enilink.komma.model.MODELS;
import net.enilink.komma.model.rdf4j.MemoryModelSetSupport;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;

import java.io.File;
import java.util.Collections;

@Iri(MODELS.NAMESPACE + "RepositoryManagerModelSet")
public abstract class RepositoryManagerModelSet extends MemoryModelSetSupport {
	EntityVar<LocalRepositoryManager> manager;

	@Iri(MODELS.NAMESPACE + "baseDir")
	public abstract Object getBaseDir();

	@Iri(MODELS.NAMESPACE + "repositoryID")
	public abstract String getRepositoryID();

	public Repository createRepository() throws RepositoryException {
		Object baseDir = getBaseDir();
		File file;
		if (baseDir instanceof IReference && (((IReference) baseDir)).getURI() != null
				&& (((IReference) baseDir)).getURI().isFile()) {
			file = new File((((IReference) baseDir)).getURI().toFileString());
		} else {
			file = new File(baseDir.toString());
		}
		LocalRepositoryManager repoManager = new LocalRepositoryManager(file) {
			@Override
			public void shutDown() {
				// prevent manager from shutting down the repositories as this already handled by KOMMA
				setInitializedRepositories(Collections.emptyMap());
				super.shutDown();
			}
		};
		repoManager.init();
		manager.set(repoManager);
		Repository repository = manager.get().getRepository(getRepositoryID());
		repository.init();
		addBasicKnowledge(repository);
		return repository;
	}

	@Override
	public void dispose() {
		LocalRepositoryManager m = manager.get();
		if (m != null) {
			m.shutDown();
		}
		manager.remove();
	}

	@Override
	public boolean isPersistent() {
		return true;
	}
}
