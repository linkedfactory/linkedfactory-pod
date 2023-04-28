package io.github.linkedfactory.service.rdf4j;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.service.rdf4j.KvinEvaluationStrategy.BNodeWithValue;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.ILiteral;
import net.enilink.komma.core.IReference;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import net.enilink.komma.literals.LiteralConverter;
import net.enilink.komma.rdf4j.RDF4JValueConverter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.helpers.SailConnectionWrapper;

public class KvinConnection extends SailConnectionWrapper {

    final KvinSail kvinSail;
    final RDF4JValueConverter valueConverter;
    final LiteralConverter literalConverter;
    private final Pattern containerMembershipPredicatePattern =
        Pattern.compile("^http://www.w3.org/1999/02/22-rdf-syntax-ns#_[1-9][0-9]*$");
    private Model newStatements = new LinkedHashModel();

    public KvinConnection(KvinSail sail, SailConnection baseConnection) {
        super(baseConnection);
        this.kvinSail = sail;
        this.valueConverter = sail.getValueConverter();
        this.literalConverter = sail.getLiteralConverter();
    }

    @Override
    public void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        if (contexts.length == 0) {
            super.addStatement(subj, pred, obj, contexts);
        } else {
            for (Resource ctx : contexts) {
                if (ctx != null && ctx.isIRI() && ((IRI) ctx).getNamespace().startsWith("kvin:")) {
                    newStatements.add(subj, pred, obj, ctx);
                } else {
                    super.addStatement(subj, pred, obj, ctx);
                }
            }
        }
    }

    @Override
    public void addStatement(UpdateContext modify, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        if (contexts.length == 0) {
            super.addStatement(modify, subj, pred, obj, contexts);
        } else {
            for (Resource ctx : contexts) {
                if (ctx != null && ctx.isIRI() && ((IRI) ctx).getNamespace().startsWith("kvin:")) {
                    newStatements.add(subj, pred, obj, ctx);
                } else {
                    super.addStatement(modify, subj, pred, obj, contexts);
                }
            }
        }
    }

    @Override
    public void flush() throws SailException {
        super.flush();
        createKvinTuples();
        newStatements = new LinkedHashModel();
    }

    private void createKvinTuples() {
        long currentTime = System.currentTimeMillis();
        kvinSail.getKvin().put(WrappedIterator.create(newStatements
            .stream().filter(stmt -> stmt.getSubject().isIRI())
            .map(stmt -> {
                IRI item = (IRI) stmt.getSubject();
                IRI predicate = stmt.getPredicate();
                return toKvinTuple(item, predicate, stmt.getObject(), currentTime);
                //System.out.println(tuple);
            }).iterator()));
    }

    private KvinTuple toKvinTuple(IRI item, IRI predicate, Value rdfValue, long currentTime) {
        long time = -1;
        int seqNr = 0;
        Object value;
        if (rdfValue.isBNode() && newStatements.contains((Resource) rdfValue, KVIN.VALUE, null)) {
            Resource r = (Resource) rdfValue;
            Iterator<Statement> it = newStatements.getStatements(r, KVIN.VALUE, null).iterator();
            value = convertValue(it.next().getObject());
            it = newStatements.getStatements(r, KVIN.TIME, null).iterator();
            if (it.hasNext()) {
                time = ((Literal) it.next().getObject()).longValue();
            }
            it = newStatements.getStatements(r, KVIN.SEQNR, null).iterator();
            if (it.hasNext()) {
                seqNr = ((Literal) it.next().getObject()).intValue();
            }
        } else if (rdfValue instanceof BNodeWithValue && ((BNodeWithValue) rdfValue).value instanceof KvinTuple) {
            KvinTuple t = (KvinTuple) ((BNodeWithValue) rdfValue).value;
            value = t.value;
            time = t.time;
            seqNr = t.seqNr;
        } else {
            value = convertValue(rdfValue);
        }
        return new KvinTuple(convertIri(item).getURI(), convertIri(predicate).getURI(),
            Kvin.DEFAULT_CONTEXT, time < 0 ? currentTime : time, seqNr, value);
    }

    private IReference convertIri(IRI rdfValue) {
        if (rdfValue.toString().startsWith("r:")) {
            return URIs.createURI(rdfValue.toString().substring(2));
        }
        return valueConverter.fromRdf4j(rdfValue);
    }

    private Object convertValue(Value rdfValue) {
        if (rdfValue.isLiteral()) {
            return literalConverter.createObject((ILiteral) valueConverter.fromRdf4j(rdfValue));
        } else if (rdfValue.isIRI()) {
            return convertIri((IRI) rdfValue);
        } else {
            // value is a blank node
            Resource r = (Resource) rdfValue;
            Iterable<Statement> stmts = newStatements.getStatements(r, null, null);
            Iterator<Statement> it = stmts.iterator();
            if (!it.hasNext()) {
                // TODO handle invalid value with exception
                return null;
            }
            List<Object> values = null;
            while (it.hasNext()) {
                Statement stmt = it.next();
                if (containerMembershipPredicatePattern.matcher(stmt.getPredicate().toString()).matches()) {
                    // value is a container
                    if (values == null) {
                        values = new ArrayList<>();
                    }
                    int index = Integer.parseInt((stmt.getPredicate()).getLocalName().substring(1));
                    values.set(index, convertValue(stmt.getObject()));
                }
            }
            if (values != null) {
                return values;
            }
            it = stmts.iterator();
            Record record = Record.NULL;
            while (it.hasNext()) {
                // value is a list of key-value pairs
                Statement stmt = it.next();
                record = record.append(new Record(valueConverter.fromRdf4j(stmt.getPredicate()).getURI(),
                    convertValue(stmt.getObject())));
            }
            return record;
        }
    }
}
