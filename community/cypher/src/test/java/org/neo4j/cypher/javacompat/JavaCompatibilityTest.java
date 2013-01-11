package org.neo4j.cypher.javacompat;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

public class JavaCompatibilityTest
{
    private GraphDatabaseService db;
    private ExecutionEngine engine;

    @Before
    public void setUp() throws IOException
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        engine = new ExecutionEngine( db );
        Transaction tx = db.beginTx();

        tx.success();
        tx.finish();
    }

    @Test
    public void collections_in_collections_look_aiight() throws Exception
    {
        ExecutionResult execute = engine.execute( "START n=node(0) RETURN [[ [1,2],[3,4] ],[[5,6]]] as x" );
        Map<String, Object> next = execute.iterator().next();
        List<List<Object>> x = (List<List<Object>>)next.get( "x" );
        Object objects = x.get( 0 );

        assertThat(objects, is(Iterable.class));
    }
}
