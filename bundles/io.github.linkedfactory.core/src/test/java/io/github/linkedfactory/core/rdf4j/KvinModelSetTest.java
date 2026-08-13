package io.github.linkedfactory.core.rdf4j;

import com.google.inject.Guice;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import net.enilink.komma.core.*;
import net.enilink.komma.core.visitor.IDataVisitor;
import net.enilink.komma.model.*;
import net.enilink.vocab.owl.Restriction;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class KvinModelSetTest {
    private HttpServer server;

    @Test
    public void testBasicConfig() {
        assertModelCreated("/kvin-modelset-config.ttl");
    }

    @Test
    public void testSearchConfig() throws IOException {
        setUp();
        assertModelCreated("/kvin-modelset-config-with-search.ttl");
        if (server != null) {
            server.stop(0);
        }
    }

    private void assertModelCreated(String configPath) {

        // create configuration and a model set factory
        KommaModule module = ModelPlugin.createModelSetModule(getClass().getClassLoader());
        IModelSetFactory factory = Guice.createInjector(new ModelSetModule(module)).getInstance(IModelSetFactory.class);

        IGraph config = new LinkedHashGraph();
        ModelUtil.readData(getClass().getResourceAsStream(configPath), null,
                "text/turtle", new IDataVisitor<Object>() {
                    @Override
                    public Object visitBegin() {
                        return null;
                    }

                    @Override
                    public Object visitEnd() {
                        return null;
                    }

                    @Override
                    public Object visitStatement(IStatement stmt) {
                        return config.add(stmt);
                    }
                });

        IModelSet modelSet = factory.createModelSet(URIs.createURI("urn:enilink:data"), config);
        Assert.assertTrue(modelSet.createModel(URIs.createURI("test:model"))
                .getManager().create(Restriction.class) instanceof Restriction);
        modelSet.dispose();
    }

    private void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(9222), 0);
        server.createContext("/_bulk", this::handleRequest);
        server.createContext("/_search", this::handleRequest);
        server.start();
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
         final byte[] BULK_SUCCESS_RESPONSE =
                "{\"errors\":false,\"items\":[]}".getBytes(StandardCharsets.UTF_8);

        exchange.sendResponseHeaders(200, BULK_SUCCESS_RESPONSE.length);
        exchange.getResponseBody().write(BULK_SUCCESS_RESPONSE);
        exchange.close();
    }

}
