package io.github.linkedfactory.core.rdf4j.shacl;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

public class RecordingShaclSail extends NotifyingSailWrapper {
    //ONLY reports
    private final Repository reportRepository;
    private final IRI reportGraph;


    public RecordingShaclSail(Sail base, ValueFactory vf, Repository rawRepository) {
        this.reportGraph = vf.createIRI("urn:linkedfactory:shacl:report");
        this.reportRepository = rawRepository;
        setBaseSail(base);
    }

    @Override
    public NotifyingSailConnection getConnection() throws SailException {
        return new RecordingShaclSailConnection(
                super.getConnection(),
                reportRepository,
                reportGraph
        );
    }


}
