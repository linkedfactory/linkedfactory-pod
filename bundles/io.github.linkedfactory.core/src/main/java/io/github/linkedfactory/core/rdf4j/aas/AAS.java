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

	IRI API_RESOLVED = SimpleValueFactory.getInstance().createIRI("aas-api:resolved");

	String ASSETADMINISTRATIONSHELL_PREFIX = "urn:aas:AssetAdministrationShell:";

	class ResolvedValue {
		public final Value value;
		public final String type;

		ResolvedValue(Value value, String type) {
			this.value = value;
			this.type = type;
		}
	}

	static String decodeUri(String uri) {
		if (uri.startsWith("urn:base64:")) {
			// decode base64-encoded id
			byte[] decodedBytes = Base64.getDecoder().decode(uri.substring("urn:base64:".length()));
			return new String(decodedBytes, StandardCharsets.UTF_8);
		}
		return uri;
	}

	static URI stringToUri(String uriString) {
		URI uri;
		try {
			uri = URIs.createURI(uriString);
		} catch (Exception e) {
			uri = null;
		}
		if (uri == null || uri.isRelative()) {
			// invalid URI, base64-encode id
			uri = URIs.createURI("urn:base64:" +
					Base64.getEncoder().encodeToString(uriString.getBytes(StandardCharsets.UTF_8)));
		}
		return uri;
	}

	static ResolvedValue resolveReference(Object value, ValueFactory vf) {
		if (value instanceof Record r) {
			String type = null;
			String idStr = null;
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
				if (type == null) {
					Object modelTypeValue = r.first(URIs.createURI(AAS_NAMESPACE + "modelType")).getValue();
					if (modelTypeValue != null) {
						type = modelTypeValue.toString();
					}
				}

				switch (type) {
					case "Submodel":
						return new ResolvedValue(vf.createIRI(stringToUri(idStr).toString()), "Submodel");
					default:
						// do not convert the reference to an IRI
				}
			}
		}
		return null;
	}
}
