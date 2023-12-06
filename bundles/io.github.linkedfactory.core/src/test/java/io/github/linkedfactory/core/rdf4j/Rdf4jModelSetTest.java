package io.github.linkedfactory.core.rdf4j;

import com.google.inject.Guice;
import net.enilink.komma.core.*;
import net.enilink.komma.core.visitor.IDataVisitor;
import net.enilink.komma.model.*;
import net.enilink.vocab.owl.Restriction;
import org.junit.Assert;
import org.junit.Test;

public class Rdf4jModelSetTest {
	@Test
	public void testBasicConfig() {
		// create configuration and a model set factory
		KommaModule module = ModelPlugin.createModelSetModule(getClass().getClassLoader());
		IModelSetFactory factory = Guice.createInjector(new ModelSetModule(module)).getInstance(IModelSetFactory.class);

		IGraph config = new LinkedHashGraph();
		ModelUtil.readData(getClass().getResourceAsStream("/rdf4j-modelset-config.ttl"), null,
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
}
