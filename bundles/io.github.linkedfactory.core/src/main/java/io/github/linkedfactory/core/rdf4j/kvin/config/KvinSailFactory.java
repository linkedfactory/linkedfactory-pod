package io.github.linkedfactory.core.rdf4j.kvin.config;

import io.github.linkedfactory.core.kvin.DelegatingKvin;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.rdf4j.kvin.KvinSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * A {@link SailFactory} that creates {@link KvinSail}s based on RDF configuration data.
 */
public class KvinSailFactory implements SailFactory {
	/**
	 * The type of repositories that are created by this factory.
	 *
	 * @see SailFactory#getSailType()
	 */
	public static final String SAIL_TYPE = "kvin:KvinSail";
	private static final Logger logger = LoggerFactory.getLogger(KvinSailFactory.class);

	/**
	 * Returns the Sail's type: <tt>kvin:KvinSail</tt>.
	 */
	@Override
	public String getSailType() {
		return SAIL_TYPE;
	}

	@Override
	public SailImplConfig getConfig() {
		return new KvinSailConfig();
	}

	@Override
	public Sail getSail(SailImplConfig config) throws SailConfigException {
		if (!SAIL_TYPE.equals(config.getType())) {
			throw new SailConfigException("Invalid Sail type: " + config.getType());
		}

		Supplier<Kvin> kvinSupplier = () -> {
			Bundle bundle = FrameworkUtil.getBundle(KvinSailFactory.class);
			BundleContext bundleContext = bundle == null ? null : bundle.getBundleContext();
			return bundleContext != null ? bundleContext.getService(bundleContext.getServiceReference(Kvin.class)) : null;
		};

		Sail sail = new KvinSail(new DelegatingKvin(kvinSupplier));
		if (config instanceof KvinSailConfig) {
			// set config values;
		} else {
			logger.warn("Config is instance of {} is not KvinSailConfig.", config.getClass().getName());
		}
		return sail;
	}
}