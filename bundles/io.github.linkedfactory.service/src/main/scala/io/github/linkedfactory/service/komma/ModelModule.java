package io.github.linkedfactory.service.komma;

import net.enilink.komma.core.KommaModule;

public class ModelModule extends KommaModule {
    {
        addBehaviour(KvinMemoryModelSet.class);
    }

}
