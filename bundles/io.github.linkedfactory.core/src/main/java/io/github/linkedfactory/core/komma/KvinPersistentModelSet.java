package io.github.linkedfactory.core.komma;

import io.github.linkedfactory.core.kvin.DelegatingKvin;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.rdf4j.kvin.KvinSail;
import net.enilink.composition.annotations.Iri;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
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

import java.io.File;
import java.net.URL;
import java.util.function.Supplier;

@Iri(MODELS.NAMESPACE + "KvinPersistentModelSet")
public abstract class KvinPersistentModelSet extends PersistentModelSetSupport {
    static BundleContext bundleContext = FrameworkUtil.getBundle(KvinPersistentModelSet.class).getBundleContext();
    static Kvin kvin;

    public Repository createRepository() throws RepositoryException {
        URI repo = getRepository();
        if (repo.scheme() == "workspace") {
            try {
                String instanceFilter = "(type=osgi.instance.area)";
                BundleContext context = FrameworkUtil.getBundle(PersistentModelSetSupport.class).getBundleContext();
                ServiceReference<?>[] refs = context
                        .getServiceReferences("org.eclipse.osgi.service.datalocation.Location", instanceFilter);
                if (refs.length > 0) {
                    Object location = context.getService(refs[0]);
                    URL loc = (URL) location.getClass().getMethod("getURL").invoke(location);
                    URI workspace = URIs.createURI(FileLocator.resolve(loc).toString());
                    if (workspace.lastSegment() == "") {
                        workspace = workspace.trimSegments(1);
                    }
                    repo = workspace.appendSegments(repo.segments());
                }
            } catch (Exception e) {
                throw new RepositoryException(e);
            }
        } else {
            throw new RepositoryException("Location service for workspace scheme not found");
        }

        NotifyingSail store = new NativeStore(new File(repo.toFileString()), "cspo,cpos,spoc,posc");
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
