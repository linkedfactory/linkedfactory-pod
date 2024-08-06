package io.github.linkedfactory.core.rdf4j;

import com.google.inject.Binding;
import com.google.inject.Key;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.http.KvinHttp;
import io.github.linkedfactory.core.rdf4j.aas.AasFederatedService;
import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import io.github.linkedfactory.core.rdf4j.kvin.KvinFederatedService;
import io.github.linkedfactory.core.rdf4j.kvin.functions.DateTimeFunction;
import net.enilink.komma.model.IModelSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.repository.Repository;
import org.osgi.service.component.annotations.*;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class FederatedServiceComponent {
	private static ExecutorService executorService;
	IModelSet ms;
	Kvin kvin;
	AbstractFederatedServiceResolver serviceResolver;
	@Reference(cardinality = ReferenceCardinality.OPTIONAL)
	volatile ContextProvider contextProvider;

	public static synchronized ExecutorService getExecutorService() {
		if (executorService == null) {
			executorService = Executors.newCachedThreadPool();
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				if (executorService != null) {
					executorService.shutdown();
				}
			}));
		}
		return executorService;
	}

	@Activate
	void activate() {
		// add custom SPARQL functions
		FunctionRegistry.getInstance().add(new DateTimeFunction());

		IModelSet.Internal msInternal = (IModelSet.Internal) ms;
		Binding<Repository> repositoryBinding = msInternal.getInjector().getExistingBinding(Key.get(Repository.class));
		if (repositoryBinding != null) {
			final Repository repository = repositoryBinding.getProvider().get();
			if (repository instanceof FederatedServiceResolverClient) {
				serviceResolver = new BaseFederatedServiceResolver() {
					@Override
					protected FederatedService createService(String serviceUrl)
							throws QueryEvaluationException {
						if (serviceUrl.startsWith("aas-api:")) {
							return new AasFederatedService(serviceUrl.replaceFirst("^aas-api:", ""), () -> getExecutorService());
						} else if (serviceUrl.equals("kvin:")) {
							return new KvinFederatedService(kvin,
									() -> getExecutorService(),
									() -> contextProvider == null ? Kvin.DEFAULT_CONTEXT : contextProvider.getContext(),
									false);
						} else if (getKvinServiceUrl(serviceUrl).isPresent()) {
							String url = getKvinServiceUrl(serviceUrl).get();
							return new KvinFederatedService(new KvinHttp(url), () -> getExecutorService(),
									() -> contextProvider == null ? Kvin.DEFAULT_CONTEXT : contextProvider.getContext(),
									true);
						}
						return null;
					}
				};
				((FederatedServiceResolverClient) repository).setFederatedServiceResolver(serviceResolver);
			}
		}
	}

	@Deactivate
	void deactivate() {
		if (serviceResolver != null) {
			serviceResolver.shutDown();
			serviceResolver = null;
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
