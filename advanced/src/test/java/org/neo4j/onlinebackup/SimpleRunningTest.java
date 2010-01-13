package org.neo4j.onlinebackup;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

/**
 * Try to backup Neo to another running Neo instance.
 */
public class SimpleRunningTest
{
    private static enum MyRels implements RelationshipType
    {
        TEST
    }

    private static final String FILE_SEP = System
        .getProperty( "file.separator" );
    private static final String TARGET_DIR = "target";
    private static final String VAR = TARGET_DIR + FILE_SEP + "var";
    private static final String STORE_LOCATION_DIR = VAR + FILE_SEP + "neo-db";
    private static final String BACKUP_LOCATION_DIR = VAR + FILE_SEP
        + "neo-backup";

    @Before
    public void clean()
    {
        Util.deleteDir( new File( VAR ) );

        System.out.println( "setting up simple database and backup-copy" );

        EmbeddedGraphDatabase neo = Util.startNeoInstance( STORE_LOCATION_DIR );
        ((NeoStoreXaDataSource) neo.getConfig().getPersistenceModule()
            .getPersistenceManager().getPersistenceSource()
            .getXaDataSource()).keepLogicalLogs( true );

        Transaction tx = neo.beginTx();
        try
        {
            addNode( neo );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        Util.stopNeo( neo );

        Util.copyDir( STORE_LOCATION_DIR, BACKUP_LOCATION_DIR );
    }

    @Test
    public void backup() throws IOException
    {
        EmbeddedGraphDatabase neo = Util.startNeoInstance( STORE_LOCATION_DIR );
        ((NeoStoreXaDataSource) neo.getConfig().getPersistenceModule()
            .getPersistenceManager().getPersistenceSource().getXaDataSource())
            .keepLogicalLogs( true );
        System.out.println( "backing up original db without any changes" );
        tryBackup( neo, BACKUP_LOCATION_DIR, 1 );

        Transaction tx = neo.beginTx();
        try
        {
            addNode( neo );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "one node added" );
        tryBackup( neo, BACKUP_LOCATION_DIR, 2 );

        tx = neo.beginTx();
        try
        {
            addNode( neo );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "one node added" );

        tx = neo.beginTx();
        try
        {
            addNode( neo );
            System.out.println( "one node added, not commited" );
            tryBackup( neo, BACKUP_LOCATION_DIR, 3 );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "previous add commited" );
        tryBackup( neo, BACKUP_LOCATION_DIR, 4 );

        Util.stopNeo( neo );
    }

    protected void tryBackup( EmbeddedGraphDatabase neo, String location, int relCount )
        throws IOException
    {
        System.out.println( "backing up to running EmbeddedGraphDatabase instance" );
        EmbeddedGraphDatabase bNeo = Util.startNeoInstance( location );
        Backup backupComp = new NeoBackup( neo, bNeo );
        backupComp.enableFileLogger();
        backupComp.doBackup();
        Util.stopNeo( bNeo );
        bNeo = Util.startNeoInstance( location );
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

    private void addNode( EmbeddedGraphDatabase neo )
    {
        Node referenceNode = neo.getReferenceNode();
        Node node = neo.createNode();
        referenceNode.createRelationshipTo( node, MyRels.TEST );
    }
}
