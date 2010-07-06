package slavetest;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class TestSlaveCreateNode
{
    @Test
    public void slaveCreateNode()
    {
        GraphDatabaseService graphdb = getSlave();
        Transaction tx = graphdb.beginTx();
        try
        {
            graphdb.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private GraphDatabaseService getSlave()
    {
        return new HighlyAvailableGraphDatabase();
    }
}
