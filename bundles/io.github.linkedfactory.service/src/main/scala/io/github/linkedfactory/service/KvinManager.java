package io.github.linkedfactory.service;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.service.config.IKvinFactory;
import io.github.linkedfactory.service.config.KvinLevelDbFactory;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import net.enilink.komma.em.concepts.IClass;
import net.enilink.komma.em.concepts.IResource;
import net.enilink.platform.core.PluginConfigModel;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;

@Component
public class KvinManager {

    static final Logger log = LoggerFactory.getLogger(KvinManager.class);

    static final URI cfgUri = URIs.createURI("plugin://io.github.linkedfactory.service/data/");
    PluginConfigModel configModel;
    ServiceRegistration<Kvin> kvinServiceRegistration;
    Kvin kvin;

    @Reference
    void setConfigModel(PluginConfigModel configModel) {
        this.configModel = configModel;
    }

    @Activate
    void activate(ComponentContext ctx) {
        try {
            configModel.begin();
            Object cfg = configModel.getManager().find(cfgUri, IResource.class)
                    .getSingle(cfgUri.appendLocalPart("store"));

            if (! (cfg instanceof IResource)) {
                log.error("Kvin store is not properly configured in: {}", cfgUri);
                return;
            }

            IResource cfgResource = (IResource) cfg;
            IKvinFactory factory;
            if (!(cfg instanceof IKvinFactory)) {
                if (cfgResource.getRdfTypes().isEmpty()) {
                    // fallback to KvinLevelDb
                    log.info("Using default KvinLevelDb as type is not specified in config: {}", cfgUri);
                    cfgResource.getRdfTypes().add(configModel.getManager()
                            .find(URIs.createURI(KvinLevelDbFactory.TYPE), IClass.class));
                    factory = cfgResource.as(IKvinFactory.class);
                } else {
                    log.error("Invalid Kvin configurations with types: {}", cfgResource.getRdfTypes());
                    return;
                }
            } else {
                factory = (IKvinFactory) cfg;
            }

            try {
                kvin = factory.create();
                kvinServiceRegistration = ctx.getBundleContext().registerService(Kvin.class, kvin, new Hashtable<>());
            } catch (Throwable throwable) {
                log.error("Failure while creating Kvin store", throwable);
            }
        } finally {
            configModel.end();
        }
    }

    @Deactivate
    void deactivate(ComponentContext ctx) {
        if (kvinServiceRegistration != null) {
            kvinServiceRegistration.unregister();
            kvinServiceRegistration = null;
        }
        if (kvin != null) {
            kvin.close();
            kvin = null;
        }
    }

}
