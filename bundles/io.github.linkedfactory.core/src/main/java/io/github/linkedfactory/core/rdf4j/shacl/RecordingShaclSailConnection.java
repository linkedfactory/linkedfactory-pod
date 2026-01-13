package io.github.linkedfactory.core.rdf4j.shacl;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;
import org.eclipse.rdf4j.sail.shacl.ShaclSailValidationException;

import java.util.ArrayList;
import java.util.List;

public class RecordingShaclSailConnection extends NotifyingSailConnectionWrapper {

    // TODO what kind of Repository
    private final Repository reportRepository;
    //private final ValueFactory vf = SimpleValueFactory.getInstance();

    private final IRI reportGraph;

    private final List<Statement> added = new ArrayList<>();
    private final List<Statement> removed = new ArrayList<>();


    public RecordingShaclSailConnection(
            NotifyingSailConnection delegate,
            Repository rawRepository, IRI reportGraph) {
        super(delegate);
        this.reportRepository = rawRepository;
        this.reportGraph = reportGraph;
        delegate.addConnectionListener(new SailConnectionListener() {

            @Override
            public void statementAdded(Statement st) {
                added.add(st);
            }

            @Override
            public void statementRemoved(Statement st) {
                removed.add(st);
            }
        });
    }


    @Override
    /* Shadow-write + SHACL mirror
       - Writes always go to the raw store first.
       - SHACL runs only on a mirror for validation + reporting
     */
    public void commit() throws SailException {
        // Phase 1: Always write to data store
        writeToRawStore();

        // Phase 2: validate via SHACL mirror
        try {
            super.commit();
            //TODO remove the data from the rawRepository?
        } catch (ShaclSailValidationException e) {
            storeValidationReport(e.validationReportAsModel());
        } finally {
            added.clear();
            removed.clear();
        }
    }

    //TODO use the rawRepository only for the SHACL Report
    // For the data use KvinConnector ?
    private void writeToRawStore() throws SailException {

        try (RepositoryConnection rc = reportRepository.getConnection()) {

            rc.begin();

            // Apply removals first
            for (Statement st : removed) {
                if (st.getContext() != null) {
                    rc.remove(
                            st.getSubject(),
                            st.getPredicate(),
                            st.getObject(),
                            st.getContext()
                    );
                } else {
                    rc.remove(
                            st.getSubject(),
                            st.getPredicate(),
                            st.getObject()
                    );
                }
            }

            // Apply additions
            for (Statement st : added) {
                if (st.getContext() != null) {
                    rc.add(
                            st.getSubject(),
                            st.getPredicate(),
                            st.getObject(),
                            st.getContext()
                    );
                } else {
                    rc.add(
                            st.getSubject(),
                            st.getPredicate(),
                            st.getObject()
                    );
                }
            }

            rc.commit();

        } catch (Exception e) {
            throw new SailException("Failed to write data to raw store", e);
        }
    }

    private void storeValidationReport(Model report) {
        try (RepositoryConnection rc = reportRepository.getConnection()) {
            rc.begin();
            rc.add(report, reportGraph);
            rc.commit();
        }
    }
}
