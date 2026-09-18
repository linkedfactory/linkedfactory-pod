package io.github.linkedfactory.core.komma;

import io.github.linkedfactory.core.kvin.DelegatingKvin;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.rdf4j.kvin.KvinSail;
import net.enilink.composition.annotations.Iri;
import net.enilink.komma.core.*;
import net.enilink.komma.model.MODELS;
import net.enilink.komma.model.rdf4j.PersistentModelSetSupport;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.inferencer.fc.SchemaCachingRDFSInferencer;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.function.Supplier;

@Iri(MODELS.NAMESPACE + "KvinPersistentModelSet")
public abstract class KvinPersistentModelSet extends PersistentModelSetSupport {
    private static final Logger log = LoggerFactory.getLogger(KvinPersistentModelSet.class);
    static BundleContext bundleContext = FrameworkUtil.getBundle(KvinPersistentModelSet.class).getBundleContext();
    static Kvin kvin;

    public Repository createRepository() throws RepositoryException {
	    IValue repo = getRepository();
	    if (repo == null) {
		    repo = getDataDir();
	    }
	    if (repo == null) {
		    throw new RepositoryException("No repository location specified");
	    }
	    String dataDir;
	    if (repo instanceof IReference && ((IReference) repo).getURI() != null) {
		    dataDir = resolveWorkspaceURI(((IReference) repo).getURI()).toFileString();
	    } else {
		    dataDir = repo instanceof ILiteral ? ((ILiteral) repo).getLabel() : repo.toString();
	    }
	    log.info("Using data directory: " + dataDir);

        NotifyingSail store = new NativeStore(new File(dataDir), "cspo,cpos,spoc,posc");
        if (! Boolean.FALSE.equals(getInference())) {
            store = new SchemaCachingRDFSInferencer(store);
        }
        Supplier<Kvin> kvinSupplier = () -> {
            if (kvin != null) {
                return kvin;
            } else {
                return bundleContext.getService(bundleContext.getServiceReference(Kvin.class));
            }
        };
        Sail kvinSail = new KvinSail(new DelegatingKvin(kvinSupplier), store);
        SailRepository repository = new SailRepository(kvinSail);
        repository.init();
        addBasicKnowledge(repository);
        return repository;
    }

    public static void setKvin(Kvin kvin) {
        KvinPersistentModelSet.kvin = kvin;
    }
}
