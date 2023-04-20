package io.github.linkedfactory.service.komma;

import io.github.linkedfactory.kvin.DelegatingKvin;
import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.service.KvinManager$;
import io.github.linkedfactory.service.rdf4j.KvinSail;
import net.enilink.composition.annotations.Iri;
import net.enilink.komma.model.MODELS;
import net.enilink.komma.model.rdf4j.MemoryModelSetSupport;
import java.util.function.Supplier;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.inferencer.fc.SchemaCachingRDFSInferencer;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

@Iri(MODELS.NAMESPACE + "KvinMemoryModelSet")
public abstract class KvinMemoryModelSet extends MemoryModelSetSupport {
    public Repository createRepository() throws RepositoryException {
        NotifyingSail store = new MemoryStore();
        if (! Boolean.FALSE.equals(getInference())) {
            store = new SchemaCachingRDFSInferencer(store);
        }
        Supplier<Kvin> kvinSupplier = () -> KvinManager$.MODULE$.getKvin().get();
        Sail kvinSail = new KvinSail(new DelegatingKvin(kvinSupplier), store);
        SailRepository repository = new SailRepository(kvinSail);
        repository.init();
        addBasicKnowledge(repository);
        return repository;
    }
}
