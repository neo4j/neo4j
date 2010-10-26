package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertEquals;
import static org.neo4j.index.Neo4jTestCase.assertContains;

import java.io.File;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestIndexNames
{
    private static GraphDatabaseService graphDb;
    private Transaction tx;
    
    @BeforeClass
    public static void setUpStuff()
    {
        String storeDir = "target/var/freshindex";
        Neo4jTestCase.deleteFileOrDirectory( new File( storeDir ) );
        graphDb = new EmbeddedGraphDatabase( storeDir, MapUtil.stringMap( "index", "lucene" ) );
    }

    @AfterClass
    public static void tearDownStuff()
    {
        graphDb.shutdown();
    }

    @After
    public void commitTx()
    {
        finishTx( true );
    }

    public void rollbackTx()
    {
        finishTx( false );
    }

    public void finishTx( boolean success )
    {
        if ( tx != null )
        {
            if ( success )
            {
                tx.success();
            }
            tx.finish();
            tx = null;
        }
    }

    public void beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
    }

    void restartTx()
    {
        finishTx( true );
        beginTx();
    }
    
    @Test
    public void makeSureIndexNamesCanBeRead()
    {
        assertEquals( 0, graphDb.index().nodeIndexNames().length );
        String name1 = "my-index-1";
        Index<Node> nodeIndex1 = graphDb.index().forNodes( name1 );
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1 );
        String name2 = "my-index-2";
        graphDb.index().forNodes( name2 );
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1, name2 );
        graphDb.index().forRelationships( name1 );
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1, name2 );
        assertContains( Arrays.asList( graphDb.index().relationshipIndexNames() ), name1 );
        
        restartTx();
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1, name2 );
        assertContains( Arrays.asList( graphDb.index().relationshipIndexNames() ), name1 );
        nodeIndex1.delete();
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1, name2 );
        assertContains( Arrays.asList( graphDb.index().relationshipIndexNames() ), name1 );
        restartTx();
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name2 );
        assertContains( Arrays.asList( graphDb.index().relationshipIndexNames() ), name1 );
    }
}
