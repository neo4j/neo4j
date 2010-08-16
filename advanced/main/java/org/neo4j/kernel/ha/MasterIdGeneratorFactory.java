package org.neo4j.kernel.ha;

import org.neo4j.kernel.CommonFactories.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class MasterIdGeneratorFactory extends DefaultIdGeneratorFactory
{
    @Override
    public void updateIdGenerators( NeoStore neoStore )
    {
        // Do nothing
    }
}
