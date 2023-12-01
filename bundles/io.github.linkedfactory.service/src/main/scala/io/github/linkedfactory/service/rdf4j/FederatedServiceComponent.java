package io.github.linkedfactory.service.rdf4j;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.http.KvinHttp;
import io.github.linkedfactory.service.rdf4j.aas.AasFederatedService;
import io.github.linkedfactory.service.rdf4j.kvin.KvinFederatedService;
import io.github.linkedfactory.service.rdf4j.kvin.functions.DateTimeFunction;
import net.enilink.komma.model.IModelSet;

import java.util.Optional;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.repository.Repository;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import com.google.inject.Binding;
import com.google.inject.Key;

@Component
public class FederatedServiceComponent {
	IModelSet ms;
	Kvin kvin;

	@Activate
	void activate() {
		// add custom SPARQL functions
		FunctionRegistry.getInstance().add(new DateTimeFunction());

		IModelSet.Internal msInternal = (IModelSet.Internal) ms;
		Binding<Repository> repositoryBinding = msInternal.getInjector().getExistingBinding(Key.get(Repository.class));
		if (repositoryBinding != null) {
			final Repository repository = repositoryBinding.getProvider().get();
			if (repository instanceof FederatedServiceResolverClient) {
				((FederatedServiceResolverClient) repository)
						.setFederatedServiceResolver(new AbstractFederatedServiceResolver() {
							@Override
							protected FederatedService createService(String serviceUrl)
									throws QueryEvaluationException {
								if (serviceUrl.startsWith("aas-api:")) {
									return new AasFederatedService(serviceUrl.replaceFirst("^aas-api:", ""));
								} else if (serviceUrl.equals("kvin:")) {
									return new KvinFederatedService(kvin, false);
								} else if (getKvinServiceUrl(serviceUrl).isPresent()) {
									String url = getKvinServiceUrl(serviceUrl).get();
									return new KvinFederatedService(new KvinHttp(url), true);
								}
								return null;
							}
						});
			}
		}
	}

	private Optional<String> getKvinServiceUrl(String serviceUrl) {
		Optional<String> url = Optional.empty();
		if (serviceUrl.startsWith("kvin:")) {
			url = Optional.of(serviceUrl.replace("kvin:", ""));
		}
		return url;
	}

	@Reference
	void setModelSet(IModelSet ms) {
		this.ms = ms;
	}

	@Reference
	void setKvin(Kvin kvin) {
		this.kvin = kvin;
	}
}
