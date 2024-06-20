package io.github.linkedfactory.service.config;

import net.enilink.komma.core.KommaModule;

public class ConfigModule extends KommaModule {
	{
		// specify dummy type to make KOMMA happy
		addConcept(IKvinFactory.class, "plugin://io.github.linkedfactory.service/data/Kvin");
		addBehaviour(KvinLevelDbFactory.class);
		addBehaviour(KvinPartitionedFactory.class);
	}
}
