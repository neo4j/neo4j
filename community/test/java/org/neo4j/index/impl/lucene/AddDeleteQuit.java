package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * This class is used by {@link TestRecovery} so that a graph database can
 * be shut down in a non-clean way after index add, then index delete.
 */
public class AddDeleteQuit
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new EmbeddedGraphDatabase( args[0] );
        Index<Node> index = db.index().forNodes( "index" );
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            index.add( node, "key", "value" );
            index.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.exit( 0 );
    }
}
