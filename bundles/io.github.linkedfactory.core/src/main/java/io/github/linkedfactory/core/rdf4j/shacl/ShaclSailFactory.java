package io.github.linkedfactory.core.rdf4j.shacl;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;

/**
 * SHACL is a behavioral layer and not a storage engine,
 * applied only for a specific federated SERVICE,
 * wrapping an already-existing Sail,
 * created programmatically inside FederatedServiceComponent
 */
public class ShaclSailFactory {
    private ShaclSailFactory() {}

    public static Sail create(NotifyingSail base, ValueFactory vf, Repository reportRepository) {

        ShaclSail shacl = new ShaclSail(base);
        return new RecordingShaclSail(shacl, vf, reportRepository);
    }
}
