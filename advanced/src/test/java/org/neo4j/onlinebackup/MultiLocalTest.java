package org.neo4j.onlinebackup;

import java.io.IOException;
import java.util.ArrayList;

import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Try to backup Neo4j and a Lucene data source to a directory location.
 */
public class MultiLocalTest extends MultiRunningTest
{
    @Override
    @SuppressWarnings( "serial" )
    protected void setupBackup( EmbeddedGraphDatabase graphDb, String location )
        throws IOException
    {
        Backup backupComp = new Neo4jBackup( graphDb, location,
            new ArrayList<String>()
            {
                {
                    add( "nioneodb" );
                    add( "lucene" );
                }
            } );
        backupComp.doBackup();
    }
}
