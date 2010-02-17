package org.neo4j.onlinebackup;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Test to backup only Neo4j to a backup location.
 */
public class SimpleLocalTest extends SimpleRunningTest
{
    @Override
    protected void tryBackup( EmbeddedGraphDatabase graphDb, String location,
        int relCount ) throws IOException
    {
        System.out.println( "backing up to backup location" );
        Backup backupComp = new Neo4jBackup( graphDb, location );
        backupComp.doBackup();
        EmbeddedGraphDatabase bDb = Util.startGraphDbInstance( location );
        Transaction bTx = bDb.beginTx();
        try
        {
            List<Relationship> rels = new ArrayList<Relationship>();
            for ( Relationship rel : bDb.getReferenceNode().getRelationships() )
            {
                rels.add( rel );
            }
            assertEquals( relCount, rels.size() );
            bTx.success();
        }
        finally
        {
            bTx.finish();
        }
        Util.stopGraphDb( bDb );
    }
}
