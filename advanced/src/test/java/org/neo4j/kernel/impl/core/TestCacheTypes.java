package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.core.NodeManager.CacheType;

public class TestCacheTypes extends AbstractNeo4jTestCase
{
    private static final String PATH = NEO4J_BASE_PATH + "cache-db";
    
    @BeforeClass
    public static void clear()
    {
        deleteFileOrDirectory( new File( PATH ) );
    }
    
    private GraphDatabaseService newDb( String cacheType )
    {
        return new EmbeddedGraphDatabase( PATH, MapUtil.stringMap( Config.CACHE_TYPE, cacheType ) );
    }
    
    @Test
    public void testDefaultCache()
    {
        GraphDatabaseService db = newDb( null );
        assertEquals( CacheType.weak, ((EmbeddedGraphDatabase) db).getConfig().getGraphDbModule().getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testWeakRefCache()
    {
        GraphDatabaseService db = newDb( "weak" );
        assertEquals( CacheType.weak, ((EmbeddedGraphDatabase) db).getConfig().getGraphDbModule().getNodeManager().getCacheType() );
        db.shutdown();
    }
    
    @Test
    public void testSoftRefCache()
    {
        GraphDatabaseService db = newDb( "soft" );
        assertEquals( CacheType.soft, ((EmbeddedGraphDatabase) db).getConfig().getGraphDbModule().getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testNoCache()
    {
        GraphDatabaseService db = newDb( "none" );
        assertEquals( CacheType.none, ((EmbeddedGraphDatabase) db).getConfig().getGraphDbModule().getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testOldCache()
    {
        GraphDatabaseService db = newDb( "old" );
        assertEquals( CacheType.old, ((EmbeddedGraphDatabase) db).getConfig().getGraphDbModule().getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testInvalidCache()
    {
        try
        {
            newDb( "whatever" );
            fail( "Should've failed" );
        }
        catch ( IllegalArgumentException e ) { /* Good */ }
    }
}
