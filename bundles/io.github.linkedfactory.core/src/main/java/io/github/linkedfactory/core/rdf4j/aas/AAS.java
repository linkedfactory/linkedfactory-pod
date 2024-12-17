package io.github.linkedfactory.core.rdf4j.aas;

import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.rdf4j.common.Conversions;
import io.github.linkedfactory.core.rdf4j.common.IRIWithValue;
import net.enilink.komma.core.URI;
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

	String ASSETADMINISTRATIONSHELL_PREFIX = "urn:aas:AssetAdministrationShell:";
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
			if (idStr == null) {
				URI keysProperty = URIs.createURI(AAS_NAMESPACE + "keys");
				Record keys = r.first(keysProperty);
				Record firstKey = null;
				// there is exactly one aas:keys element
				if (keys.getValue() instanceof Record && keys.next().first(keysProperty).getValue() == null) {
					firstKey = (Record) keys.getValue();
				} else if (keys.getValue() instanceof List<?> && ((List<?>) keys.getValue()).size() == 1) {
					firstKey = (Record) ((List<?>) keys.getValue()).get(0);
				}
				if (firstKey != null) {
					Object typeValue = firstKey.first(URIs.createURI(AAS_NAMESPACE + "type")).getValue();
					if (typeValue != null) {
						type = typeValue.toString();
					}
					Object keyValue = firstKey.first(URIs.createURI(AAS_NAMESPACE + "value")).getValue();
					if (keyValue != null) {
						idStr = keyValue.toString();
					}
				}

				if (idStr != null) {
					switch (type) {
						case "Submodel":
							return vf.createIRI("urn:aas:" + type + ":" +
									Base64.getEncoder().encodeToString(idStr.getBytes(StandardCharsets.UTF_8)));
						default:
							// do not convert the reference to an IRI
							// drop id string
							idStr = null;
					}
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

				switch (type) {
					case "Submodel":
						String iriStr = "urn:aas:" + type + ":" +
								Base64.getEncoder().encodeToString(idStr.getBytes(StandardCharsets.UTF_8));
						return IRIWithValue.create(iriStr, value);
					default:
						// do not convert the reference to an IRI
				}
			}
		}
		return Conversions.toRdfValue(value, vf, true);
	}
}
