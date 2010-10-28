package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.management.Primitives;

public class TestLoadExtensions
{
    private EmbeddedGraphDatabase db;

    @Before
    public void startDb()
    {
        db = new EmbeddedGraphDatabase( "target/var/ext" );
    }
    
    @After
    public void shutdownDb()
    {
        db.shutdown();
    }
    
    @Test
    public void makeSureJmxExtensionCanBeLoadedAndQueried()
    {
        Primitives primitivesBean = ((AbstractGraphDatabase) db).getManagementBean( Primitives.class );
        
        // Just assert so that no exception is thrown.
        primitivesBean.getNumberOfNodeIdsInUse();
    }
}
