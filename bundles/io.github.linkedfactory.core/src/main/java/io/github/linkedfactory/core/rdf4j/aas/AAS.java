package io.github.linkedfactory.core.rdf4j.aas;

import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.rdf4j.common.Conversions;
import io.github.linkedfactory.core.rdf4j.common.IRIWithValue;
import net.enilink.komma.core.URIs;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public interface AAS {
	String AAS_NAMESPACE = "https://admin-shell.io/aas/3/0/";

	IRI API_PARAMS = SimpleValueFactory.getInstance().createIRI("aas-api:params");

	IRI API_SHELLS = SimpleValueFactory.getInstance().createIRI("aas-api:shells");
	IRI API_SUBMODELS = SimpleValueFactory.getInstance().createIRI("aas-api:submodels");

	String SUBMODEL_PREFIX = "urn:aas:Submodel:";

	static Value toRdfValue(Object value, ValueFactory vf) {
		if (value instanceof Record) {
			Record r = (Record) value;
			Record id = r.first(URIs.createURI(AAS_NAMESPACE + "id"));
			String type = null;
			String idStr = null;
			if (id != Record.NULL) {
				idStr = id.getValue() != null ? id.getValue().toString() : null;
			}
			if (idStr == null && "ModelReference".equals(r.first(URIs.createURI(AAS_NAMESPACE + "type")).getValue())) {
				Object keys = r.first(URIs.createURI(AAS_NAMESPACE + "keys")).getValue();
				if (keys instanceof List<?> && ((List<?>) keys).size() == 1) {
					Record firstKey = (Record) ((List<?>) keys).get(0);
					Object typeValue = firstKey.first(URIs.createURI(AAS_NAMESPACE + "type")).getValue();
					if (typeValue != null) {
						type = typeValue.toString();
					}
					Object keyValue = firstKey.first(URIs.createURI(AAS_NAMESPACE + "value")).getValue();
					if (keyValue != null) {
						idStr = keyValue.toString();
					}
				}

				if (type != null && idStr != null) {
					// System.out.println("ref: " + "urn:aas:" + type + ":" + idStr);
					return vf.createIRI("urn:aas:" + type + ":" +
							Base64.getEncoder().encodeToString(idStr.getBytes(StandardCharsets.UTF_8)));
				}
			}

			if (idStr != null) {
				if (type == null) {
					Object modelTypeValue = r.first(URIs.createURI(AAS_NAMESPACE + "modelType")).getValue();
					if (modelTypeValue != null) {
						type = modelTypeValue.toString();
					} else {
						Object kindValue = r.first(URIs.createURI(AAS_NAMESPACE + "kind")).getValue();
						if ("Instance".equals(kindValue)) {
							type = "Submodel";
						} else if ("Template".equals(kindValue)) {
							type = "Template";
						}
					}
				}

				if (type != null) {
					String iriStr = "urn:aas:" + type + ":" +
							Base64.getEncoder().encodeToString(idStr.getBytes(StandardCharsets.UTF_8));
					// System.out.println("with value: " + iriStr);
					return IRIWithValue.create(iriStr, value);
				}
			}
		}
		return Conversions.toRdfValue(value, vf, true);
	}
}
