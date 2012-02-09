package org.neo4j.graphalgo.ancestor;

import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;

public class AncestorTest implements GraphHolder
{
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );
    protected static ImpermanentGraphDatabase db;

    public static enum RelTypes implements RelationshipType
    {
        PARENT_OF
    }
    
    @Test
    @Graph({"A PARENT_OF B", "B PARENT_OF C"})
    public void testSetup()
    {
        assertTrue(data.get().get("A").hasRelationship( RelTypes.PARENT_OF));
    }
    
    @BeforeClass
    public static void init()
    {
        db = new ImpermanentGraphDatabase();
    }
    
    @AfterClass
    public static void shutdownDb()
    {
        try
        {
            if ( db != null ) db.shutdown();
        }
        finally
        {
            db = null;
        }
    }

    @Before
    public void setUp()
    {
        db.cleanContent();
    }


    @Override
    public GraphDatabaseService graphdb()
    {
        return db;
    }
}
