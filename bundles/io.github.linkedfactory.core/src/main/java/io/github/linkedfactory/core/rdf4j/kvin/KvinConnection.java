package io.github.linkedfactory.core.rdf4j.kvin;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.rdf4j.common.HasValue;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.ILiteral;
import net.enilink.komma.core.IReference;
import net.enilink.komma.core.URIs;
import net.enilink.komma.literals.LiteralConverter;
import net.enilink.komma.rdf4j.RDF4JValueConverter;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.helpers.SailConnectionWrapper;

import java.util.*;
import java.util.regex.Pattern;

public class KvinConnection extends SailConnectionWrapper {

	final KvinSail kvinSail;
	final RDF4JValueConverter valueConverter;
	final LiteralConverter literalConverter;
	private static final String KVIN_NS = "kvin:";
	private final Pattern containerMembershipPredicatePattern =
			Pattern.compile("^http://www.w3.org/1999/02/22-rdf-syntax-ns#_[1-9][0-9]*$");
	private final Map<Resource, List<Statement>> stmtsBySubject = new LinkedHashMap<>();

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
			var vf = kvinSail.getValueFactory();
			for (Resource ctx : contexts) {
				if (ctx != null && ctx.isIRI() && ((IRI) ctx).getNamespace().startsWith(KVIN_NS)) {
					String newCtx = ctx.stringValue().substring(KVIN_NS.length());
					stmtsBySubject.computeIfAbsent(subj, key -> new ArrayList<>()).add(
							vf.createStatement(subj, pred, obj, newCtx.isEmpty() ? null : vf.createIRI(newCtx)));
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
			var vf = kvinSail.getValueFactory();
			for (Resource ctx : contexts) {
				if (ctx != null && ctx.isIRI() && ((IRI) ctx).getNamespace().startsWith(KVIN_NS)) {
					String newCtx = ctx.stringValue().substring(KVIN_NS.length());
					stmtsBySubject.computeIfAbsent(subj, key -> new ArrayList<>()).add(
							vf.createStatement(subj, pred, obj, newCtx.isEmpty() ? null : vf.createIRI(newCtx)));
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
		stmtsBySubject.clear();
	}

	private void createKvinTuples() {
		long currentTime = System.currentTimeMillis();
		try (IExtendedIterator<KvinTuple> tuples = WrappedIterator.create(
				stmtsBySubject.entrySet().stream().filter(e -> e.getKey().isIRI())
						.flatMap(e -> {
							IRI item = (IRI) e.getKey();
							return e.getValue().stream().map(stmt -> {
								IRI predicate = stmt.getPredicate();
								return toKvinTuple(item, predicate, stmt.getObject(), currentTime, stmt.getContext());
							});
							//System.out.println(tuple);
						}).iterator())) {
			if (tuples.hasNext()) {
				kvinSail.getKvin().put(tuples);
			}
		}
	}

	private KvinTuple toKvinTuple(IRI item, IRI predicate, Value rdfValue, long currentTime, Resource context) {
		long time = -1;
		int seqNr = 0;
		Object value = null;
		if (rdfValue.isBNode()) {
			if (rdfValue instanceof HasValue && ((HasValue) rdfValue).getValue() instanceof KvinTuple t) {
				value = t.value;
				time = t.time;
				seqNr = t.seqNr;
			} else {
				List<Statement> stmts = stmtsBySubject.get(rdfValue);
				if (stmts != null) {
					for (Statement stmt : stmts) {
						if (KVIN.VALUE.equals(stmt.getPredicate())) {
							value = convertValue(stmt.getObject());
						} else if (KVIN.TIME.equals(stmt.getPredicate())) {
							time = ((Literal) stmt.getObject()).longValue();
						} else if (KVIN.SEQNR.equals(stmt.getPredicate())) {
							seqNr = ((Literal) stmt.getObject()).intValue();
						}
					}
				}
			}
		}
		if (value == null) {
			value = convertValue(rdfValue);
		}
		return new KvinTuple(convertIri(item).getURI(), convertIri(predicate).getURI(),
				context != null && context.isIRI() ? convertIri((IRI) context).getURI() : Kvin.DEFAULT_CONTEXT,
				time < 0 ? currentTime : time, seqNr, value);
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
			List<Statement> stmts = stmtsBySubject.get(rdfValue);
			if (stmts == null || stmts.isEmpty()) {
				// TODO handle invalid value with exception
				return null;
			}
			List<Object> values = null;
			Iterator<Statement> it = stmts.iterator();
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
