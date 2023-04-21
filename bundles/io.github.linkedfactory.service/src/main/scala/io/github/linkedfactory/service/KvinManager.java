package io.github.linkedfactory.service;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import net.enilink.komma.em.concepts.IResource;
import net.enilink.platform.core.PluginConfigModel;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Hashtable;

import org.eclipse.core.runtime.Platform;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

@Component
public class KvinManager {

    static final URI cfgUri = URIs.createURI("plugin://de.fraunhofer.iwu.linkedfactory.service/data/");
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
            IResource cfg = configModel.getManager().find(cfgUri.appendLocalPart("store"), IResource.class);
            Object type = cfg.getSingle(cfgUri.appendLocalPart("type"));
            if (type == null || "KvinLevelDb".equals(type)) {
                String dirName = (String) cfg.getSingle(cfgUri.appendLocalPart("dirName"));
                if (dirName == null) {
                    dirName = "linkedfactory-valuestore";
                }
                File valueStorePath;
                if (new File(dirName).isAbsolute()) {
                    valueStorePath = new File(dirName);
                } else {
                    try {
                        valueStorePath = new File(new File(Platform.getInstanceLocation().getURL().toURI()), dirName);
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    kvin = new KvinLevelDb(valueStorePath);
                    kvinServiceRegistration = ctx.getBundleContext().registerService(Kvin.class, kvin, new Hashtable<>());
                    System.out.println("Value store: using LF w/ path=" + valueStorePath);
                } catch (Throwable throwable) {
                    System.err.println("Value store: FAILURE for LF w/ path=" + valueStorePath + ": " + throwable.getMessage());
                }
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
