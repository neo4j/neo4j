package org.neo4j.onlinebackup;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

/**
 * Try to backup Neo to another running Neo instance.
 */
public class TestApplyNewLogs
{
    private static final String FILE_SEP = System
        .getProperty( "file.separator" );
    private static final String TARGET_DIR = "target";
    private static final String VAR = TARGET_DIR + FILE_SEP + "var";
    private static final String STORE_LOCATION_DIR = VAR + FILE_SEP + "neo-db";
    private static final String BACKUP_LOCATION_DIR = VAR + FILE_SEP
        + "neo-backup";

    @Before
    public void clean() throws IOException
    {
        Util.deleteDir( new File( VAR ) );

        System.out.println( "setting up simple database and backup-copy" );

        EmbeddedGraphDatabase graphDb = 
            new EmbeddedGraphDatabase( STORE_LOCATION_DIR );
        graphDb.shutdown();
        Util.copyDir( STORE_LOCATION_DIR, BACKUP_LOCATION_DIR );
        graphDb = new EmbeddedGraphDatabase( STORE_LOCATION_DIR );
        IndexService index = new LuceneIndexService( graphDb );
        XaDataSourceManager xaDsMgr = 
            graphDb.getConfig().getTxModule().getXaDataSourceManager();
        for ( XaDataSource xaDs : xaDsMgr.getAllRegisteredDataSources() )
        {
            xaDs.keepLogicalLogs( true );
        }
        Transaction tx = graphDb.beginTx();
        try
        {
            Node node1 = graphDb.createNode();
            index.index( node1, "backup_test", "1" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        for ( XaDataSource xaDs : xaDsMgr.getAllRegisteredDataSources() )
        {
            xaDs.rotateLogicalLog();
        }
        tx = graphDb.beginTx();
        try
        {
            Node node2 = graphDb.createNode();
            index.index( node2, "backup_test", "2" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        index.shutdown();
        graphDb.shutdown();
        
        Util.copyLogs( STORE_LOCATION_DIR, BACKUP_LOCATION_DIR );
    }

    @Test
    public void backup() throws IOException
    {
        String dest = "target/var/neo-backup";
        ApplyNewLogs.main( new String[] { dest } );
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( dest );
        IndexService index = new LuceneIndexService( graphDb );
        Transaction tx = graphDb.beginTx();
        try
        {
            Assert.assertNotNull( index.getSingleNode( "backup_test", "1" ) );
            Assert.assertNotNull( index.getSingleNode( "backup_test", "2" ) );
        }
        finally
        {
            tx.finish();
        }
        index.shutdown();
        graphDb.shutdown();
    }
}
