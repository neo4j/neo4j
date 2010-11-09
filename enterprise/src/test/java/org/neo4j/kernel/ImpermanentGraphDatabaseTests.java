package org.neo4j.kernel;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;

import static junit.framework.Assert.assertEquals;

public class ImpermanentGraphDatabaseTests
{
    private GraphDatabaseService db;

    @Before
	public void Given() {
		db = new ImpermanentGraphDatabase( "neodb" );
	}

	@Test
    public void should_keep_data_between_start_and_shutdown()
    {
        createNode();
        
        assertEquals( "Expected one new node, plus reference node", 2, nodeCount() );
    }

    @Test
    public void data_should_not_survive_shutdown()
    {
        createNode();
        db.shutdown();

        db = new ImpermanentGraphDatabase( "neodb");
        
        assertEquals( "Should not see anything but the default reference node.", 1, nodeCount() );
    }
    
	private int nodeCount() {
		return IteratorUtil.count(db.getAllNodes());
	}

	private void createNode() {
		Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
	}
}
