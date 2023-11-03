package io.github.linkedfactory.service.rdf4j.aas;

import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.service.rdf4j.common.Conversions;
import io.github.linkedfactory.service.rdf4j.common.IRIWithValue;
import net.enilink.komma.core.URIs;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.util.Collection;
import java.util.List;

public interface AAS {
	String AAS = "aas:";

	IRI PARAMS = SimpleValueFactory.getInstance().createIRI(AAS + "params");

	IRI SHELLS = SimpleValueFactory.getInstance().createIRI(AAS + "shells");
	IRI SUBMODELS = SimpleValueFactory.getInstance().createIRI(AAS + "submodels");

	static Value toRdfValue(Object value, ValueFactory vf) {
		if (value instanceof Record) {
			Record r = (Record) value;
			Record id = r.first(URIs.createURI("id"));
			String idStr = null;
			if (id != Record.NULL) {
				idStr = id.getValue() != null ? id.getValue().toString() : null;
			}
			if (idStr == null && "ModelReference".equals(r.first(URIs.createURI("type")).getValue())) {
				Object keys = r.first(URIs.createURI("keys")).getValue();
				if (keys instanceof List<?> && ((List<?>) keys).size() == 1) {
					Record firstKey = (Record) ((List<?>) keys).get(0);
					Object keyValue = firstKey.first(URIs.createURI("value")).getValue();
					if (keyValue != null) {
						idStr = keyValue.toString();
					}
				}
			}
			if (idStr != null) {
				String iriStr = null;
				int colonIdx = idStr.indexOf(':');
				if (colonIdx >= 0) {
					String scheme = idStr.substring(0, colonIdx);
					if (URIs.validScheme(scheme)) {
						iriStr = idStr;
					}
				}
				if (iriStr == null) {
					iriStr = "r:" + idStr;
				}
				return IRIWithValue.create(iriStr, value);
			}
		}
		return Conversions.toRdfValue(value, vf);
	}
}
