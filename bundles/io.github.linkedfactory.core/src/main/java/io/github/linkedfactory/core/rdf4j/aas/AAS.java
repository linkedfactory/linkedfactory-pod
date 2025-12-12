package io.github.linkedfactory.core.rdf4j.aas;

import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.rdf4j.common.Conversions;
import io.github.linkedfactory.core.rdf4j.common.IRIWithValue;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;

public interface AAS {
	String AAS_NAMESPACE = "https://admin-shell.io/aas/3/0/";

	IRI API_PARAMS = SimpleValueFactory.getInstance().createIRI("aas-api:params");

	IRI API_SHELLS = SimpleValueFactory.getInstance().createIRI("aas-api:shells");
	IRI API_SUBMODELS = SimpleValueFactory.getInstance().createIRI("aas-api:submodels");

	IRI API_RESOLVED = SimpleValueFactory.getInstance().createIRI("aas-api:resolved");

	IRI PROPERTY_KEYS = SimpleValueFactory.getInstance().createIRI(AAS_NAMESPACE + "keys");
	IRI PROPERTY_TYPE = SimpleValueFactory.getInstance().createIRI(AAS_NAMESPACE + "type");
	IRI PROPERTY_VALUE = SimpleValueFactory.getInstance().createIRI(AAS_NAMESPACE + "value");
	IRI PROPERTY_MODELTYPE = SimpleValueFactory.getInstance().createIRI(AAS_NAMESPACE + "modelType");

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

	static String stringToUri(String uriString) {
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
		return uri.toString();
	}

	static ResolvedValue resolveReference(Model data, Resource reference, ValueFactory vf) {
		String type = null;
		String idStr = null;

		Set<Value> keys = data.filter(reference, PROPERTY_KEYS, null).objects();
		Value firstKey = keys.size() == 1 ? keys.iterator().next() : null;
		if (firstKey instanceof Resource) {
			Value typeValue = data.filter((Resource) firstKey, PROPERTY_TYPE, null)
					.objects().stream().findFirst().orElse(null);
			if (typeValue != null) {
				type = typeValue.stringValue();
			}
			Value keyValue = data.filter((Resource) firstKey, PROPERTY_VALUE, null)
					.objects().stream().findFirst().orElse(null);
			if (keyValue != null) {
				idStr = keyValue.stringValue();
			}
		}

		if (idStr != null) {
			if (type == null) {
				Value modelTypeValue = data.filter((Resource) firstKey, PROPERTY_MODELTYPE, null)
						.objects().stream().findFirst().orElse(null);
				if (modelTypeValue != null) {
					type = modelTypeValue.stringValue();
				}
			}

			switch (type) {
				case "Submodel":
					return new ResolvedValue(vf.createIRI(stringToUri(idStr).toString()), "Submodel");
				default:
					// do not convert the reference to an IRI
			}
		}
		return null;
	}
}
