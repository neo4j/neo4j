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
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneDataSource;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

/**
 * Try to backup Neo and a Lucene data source to another running Neo+Lucene.
 */
public class MultiRunningTest
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
    public void setup()
    {
        Util.deleteDir( new File( VAR ) );

        System.out
            .println( "setting up database and backup-copy including Lucene" );

        EmbeddedGraphDatabase neo = Util.startNeoInstance( STORE_LOCATION_DIR );
        XaDataSource neoStoreXaDataSource = neo.getConfig()
            .getPersistenceModule().getPersistenceManager()
            .getPersistenceSource().getXaDataSource();
        neoStoreXaDataSource.keepLogicalLogs( true );

        IndexService indexService = new LuceneIndexService( neo );
        XaDataSourceManager xaDsm = neo.getConfig().getTxModule()
            .getXaDataSourceManager();
        XaDataSource ds = xaDsm.getXaDataSource( "lucene" );
        ((LuceneDataSource) ds).keepLogicalLogs( true );

        Transaction tx = neo.beginTx();
        try
        {
            indexService.index( addNode( neo ), "number", 1 );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        Util.stopNeo( neo, indexService );

        Util.copyDir( STORE_LOCATION_DIR, BACKUP_LOCATION_DIR );
    }

    @Test
    public void backup() throws IOException
    {
        System.out.println( "starting tests" );
        EmbeddedGraphDatabase neo = Util.startNeoInstance( STORE_LOCATION_DIR );
        ((NeoStoreXaDataSource) neo.getConfig().getPersistenceModule()
            .getPersistenceManager().getPersistenceSource()
            .getXaDataSource()).keepLogicalLogs( true );
        IndexService indexService = new LuceneIndexService( neo );
        XaDataSourceManager xaDsm = neo.getConfig().getTxModule()
            .getXaDataSourceManager();
        XaDataSource ds = xaDsm.getXaDataSource( "lucene" );
        ((LuceneDataSource) ds).keepLogicalLogs( true );

        System.out.println( "backing up original db without any changes" );
        tryBackup( neo, BACKUP_LOCATION_DIR, 1 );

        Transaction tx = neo.beginTx();
        try
        {
            indexService.index( addNode( neo ), "number", 2 );
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
            indexService.index( addNode( neo ), "number", 3 );
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
            indexService.index( addNode( neo ), "number", 4 );
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

        Util.stopNeo( neo, indexService );
    }

    protected void tryBackup( EmbeddedGraphDatabase neo, String location, int relCount )
        throws IOException
    {
        setupBackup( neo, location );

        EmbeddedGraphDatabase bNeo = Util.startNeoInstance( location );
        IndexService bIndexService = new LuceneIndexService( bNeo );
        Transaction bTx = bNeo.beginTx();
        try
        {
            List<Relationship> rels = new ArrayList<Relationship>();
            for ( Relationship rel : bNeo.getReferenceNode().getRelationships() )
            {
                rels.add( rel );
            }
            assertEquals( relCount, rels.size() );
            Node node = bIndexService.getSingleNode( "number", relCount );
            assertEquals( true, node != null );
            assertEquals( node.getId(), (long) (Long) node.getProperty(
                "theId", -1L ) );
            bTx.success();
        }
        finally
        {
            bTx.finish();
        }
        Util.stopNeo( bNeo, bIndexService );
    }

    @SuppressWarnings( "serial" )
    protected void setupBackup( EmbeddedGraphDatabase neo, String location )
        throws IOException
    {
        EmbeddedGraphDatabase bNeo = Util.startNeoInstance( location );
        IndexService bIndexService = new LuceneIndexService( bNeo );
        Backup backupComp = new Neo4jBackup( neo, bNeo, new ArrayList<String>()
        {
            {
                add( "nioneodb" );
                add( "lucene" );
            }
        } );
        backupComp.enableFileLogger();
        backupComp.doBackup();
        Util.stopNeo( bNeo, bIndexService );
    }

    private Node addNode( EmbeddedGraphDatabase neo )
    {
        Node referenceNode = neo.getReferenceNode();
        Node node = neo.createNode();
        node.setProperty( "theId", node.getId() );
        referenceNode.createRelationshipTo( node, MyRels.TEST );
        return node;
    }
}
