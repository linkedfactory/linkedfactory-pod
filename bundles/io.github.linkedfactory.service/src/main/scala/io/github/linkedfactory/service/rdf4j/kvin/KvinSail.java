package io.github.linkedfactory.service.rdf4j.kvin;

import io.github.linkedfactory.core.kvin.Kvin;
import net.enilink.komma.core.KommaModule;
import net.enilink.komma.em.ManagerCompositionModule;
import net.enilink.komma.literals.LiteralConverter;
import net.enilink.komma.rdf4j.RDF4JValueConverter;

import java.util.Locale;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.SailWrapper;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class KvinSail extends SailWrapper {

    protected final Kvin kvin;
    protected Injector injector;
    protected RDF4JValueConverter valueConverter;
    protected LiteralConverter literalConverter;

    public KvinSail(Kvin kvin) {
        this.kvin = kvin;
    }

    public KvinSail(Kvin kvin, Sail baseSail) {
        this(kvin);
        setBaseSail(baseSail);
    }

    @Override
    public void init() throws SailException {
        super.init();
        injector = Guice.createInjector(new ManagerCompositionModule(new KommaModule()),
            new AbstractModule() {
                @Override
                protected void configure() {
                    bind(Locale.class).toInstance(Locale.getDefault());
                    bind(ValueFactory.class).toInstance(getValueFactory());
                }
            });
        valueConverter = injector.getInstance(RDF4JValueConverter.class);
        literalConverter = injector.getInstance(LiteralConverter.class);
    }

    public RDF4JValueConverter getValueConverter() {
        return valueConverter;
    }

    public LiteralConverter getLiteralConverter() {
        return literalConverter;
    }

    @Override
    public SailConnection getConnection() throws SailException {
        return new KvinConnection(this, super.getConnection());
    }

    public Kvin getKvin() {
        return kvin;
    }
}
