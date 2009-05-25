package org.neo4j.onlinebackup;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.Transaction;

/**
 * Test to backup only Neo to a backup location.
 */
public class SimpleLocalTest extends SimpleRunningTest
{
    @Override
    protected void tryBackup( EmbeddedNeo neo, String location, int relCount )
        throws IOException
    {
        System.out.println( "backing up to backup location" );
        Backup backupComp = new NeoBackup( neo, location );
        backupComp.doBackup();
        EmbeddedNeo bNeo = Util.startNeoInstance( location );
        Transaction bTx = bNeo.beginTx();
        try
        {
            List<Relationship> rels = new ArrayList<Relationship>();
            for ( Relationship rel : bNeo.getReferenceNode().getRelationships() )
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
        Util.stopNeo( bNeo );
    }
}
