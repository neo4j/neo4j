package org.neo4j.remote.test;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.Callable;

import org.junit.Test;
import org.neo4j.remote.AbstractTestBase;
import org.neo4j.remote.RemoteGraphDatabase;

public class IndexTest extends AbstractTestBase
{
    public IndexTest( Callable<RemoteGraphDatabase> factory )
    {
        super( factory );
    }

    public @Test
    void canRetreiveIndexServiceInstance()
    {
        assertNotNull( indexService() );
    }
}
