package org.neo4j.kernel.impl.core;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.GraphTransactionRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.fail;

public class GraphPropertiesProxyTest
{
    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public GraphTransactionRule tx = new GraphTransactionRule( db );

    @Test
    public void testGraphAddPropertyWithNullKey(){
        try
        {
            graphProperties().setProperty( null, "bar" );
            fail( "Null key should result in exception." );
        }
        catch ( IllegalArgumentException ignored )
        {
        }
    }

    @Test
    public void testGraphAddPropertyWithNullValue(){
        try
        {
            graphProperties().setProperty( "foo", null);
            fail( "Null value should result in exception." );
        }
        catch ( IllegalArgumentException ignored )
        {
        }
        tx.failure();
    }

    private GraphProperties graphProperties()
    {
        return db.getDependencyResolver().resolveDependency( NodeManager.class ).newGraphProperties();
    }
}
