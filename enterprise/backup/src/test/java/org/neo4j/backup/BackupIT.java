/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup;

import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Integer.parseInt;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;

@RunWith( Parameterized.class )
public class BackupIT
{
    private final TestDirectory testDir = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDir )
            .around( fileSystemRule )
            .around( pageCacheRule )
            .around( SuppressOutput.suppressAll() )
            .around( random );

    @Parameter
    public String recordFormatName;

    private File serverPath;
    private File otherServerPath;
    private File backupPath;
    private List<ServerInterface> servers;

    @Parameters( name = "{0}" )
    public static List<String> recordFormatNames()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    @Before
    public void before()
    {
        servers = new ArrayList<>();
        serverPath = testDir.directory( "server" );
        otherServerPath = testDir.directory( "server2" );
        backupPath = testDir.directory( "backedup-serverdb" );
    }

    @After
    public void shutDownServers()
    {
        for ( ServerInterface server : servers )
        {
            server.shutdown();
        }
        servers.clear();
    }

    @Test
    public void makeSureFullFailsWhenDbExists()
    {
        int backupPort = PortAuthority.allocatePort();
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath, backupPort );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
        createInitialDataSet( backupPath );
        try
        {
            backup.full( backupPath.getPath() );
            fail( "Shouldn't be able to do full backup into existing db" );
        }
        catch ( Exception e )
        {
            // good
        }
        shutdownServer( server );
    }

    @Test
    public void makeSureIncrementalFailsWhenNoDb()
    {
        int backupPort = PortAuthority.allocatePort();
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath, backupPort );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
        try
        {
            backup.incremental( backupPath.getPath() );
            fail( "Shouldn't be able to do incremental backup into non-existing db" );
        }
        catch ( Exception e )
        {
            // Good
        }
        shutdownServer( server );
    }

    @Test
    public void backedUpDatabaseContainsChecksumOfLastTx() throws Exception
    {
        ServerInterface server = null;
        try
        {
            createInitialDataSet( serverPath );
            int backupPort = PortAuthority.allocatePort();
            server = startServer( serverPath, backupPort );
            OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
            backup.full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            shutdownServer( server );
            server = null;
            PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );

            long firstChecksum = lastTxChecksumOf( serverPath, pageCache );
            assertEquals( firstChecksum, lastTxChecksumOf( backupPath, pageCache ) );

            addMoreData( serverPath );
            server = startServer( serverPath, backupPort );
            backup.incremental( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            shutdownServer( server );
            server = null;

            long secondChecksum = lastTxChecksumOf( serverPath, pageCache );
            assertEquals( secondChecksum, lastTxChecksumOf( backupPath, pageCache ) );
            assertTrue( firstChecksum != secondChecksum );
        }
        finally
        {
            if ( server != null )
            {
                shutdownServer( server );
            }
        }
    }

    @Test
    public void fullThenIncremental()
    {
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        int backupPort = PortAuthority.allocatePort();
        ServerInterface server = startServer( serverPath, backupPort );

        OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
        backup.full( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( initialDataSetRepresentation, getDbRepresentation() );
        shutdownServer( server );

        DbRepresentation furtherRepresentation = addMoreData( serverPath );
        server = startServer( serverPath, backupPort );
        backup.incremental( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( furtherRepresentation, getDbRepresentation() );
        shutdownServer( server );
    }

    @Test
    public void makeSureNoLogFileRemains()
    {
        createInitialDataSet( serverPath );
        int backupPort = PortAuthority.allocatePort();
        ServerInterface server = startServer( serverPath, backupPort );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );

        // First check full
        backup.full( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertFalse( checkLogFileExistence( backupPath.getPath() ) );
        // Then check empty incremental
        backup.incremental( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertFalse( checkLogFileExistence( backupPath.getPath() ) );
        // Then check real incremental
        shutdownServer( server );
        addMoreData( serverPath );
        server = startServer( serverPath, backupPort );
        backup.incremental( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertFalse( checkLogFileExistence( backupPath.getPath() ) );
        shutdownServer( server );
    }

    @Test
    public void makeSureStoreIdIsEnforced()
    {
        // Create data set X on server A
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        int backupPort = PortAuthority.allocatePort();
        ServerInterface server = startServer( serverPath, backupPort );

        // Grab initial backup from server A
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
        backup.full( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( initialDataSetRepresentation, getDbRepresentation() );
        shutdownServer( server );

        // Create data set X+Y on server B
        createInitialDataSet( otherServerPath );
        addMoreData( otherServerPath );
        server = startServer( otherServerPath, backupPort );

        // Try to grab incremental backup from server B.
        // Data should be OK, but store id check should prevent that.
        try
        {
            backup.incremental( backupPath.getPath() );
            fail( "Shouldn't work" );
        }
        catch ( RuntimeException e )
        {
            assertThat(e.getCause(), instanceOf(MismatchingStoreIdException.class));
        }
        shutdownServer( server );
        // Just make sure incremental backup can be received properly from
        // server A, even after a failed attempt from server B
        DbRepresentation furtherRepresentation = addMoreData( serverPath );
        server = startServer( serverPath, backupPort );
        backup.incremental( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( furtherRepresentation, getDbRepresentation() );
        shutdownServer( server );
    }

    @Test
    public void multipleIncrementals() throws Exception
    {
        int backupPort = PortAuthority.allocatePort();
        GraphDatabaseService db = null;
        try
        {
            db = getEmbeddedTestDataBaseService( backupPort );

            Index<Node> index;
            try ( Transaction tx = db.beginTx() )
            {
                index = db.index().forNodes( "yo" );
                index.add( db.createNode(), "justTo", "commitATx" );
                db.createNode();
                tx.success();
            }

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
            backup.full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
            long lastCommittedTx = getLastCommittedTx( backupPath.getPath(), pageCache );

            for ( int i = 0; i < 5; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = db.createNode();
                    index.add( node, "key", "value" + i );
                    tx.success();
                }
                backup = backup.incremental( backupPath.getPath() );
                assertTrue( "Should be consistent", backup.isConsistent() );
                assertEquals( lastCommittedTx + i + 1, getLastCommittedTx( backupPath.getPath(), pageCache ) );
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    @Test
    public void backupIndexWithNoCommits()
    {
        int backupPort = PortAuthority.allocatePort();
        GraphDatabaseService db = null;
        try
        {
            db = getEmbeddedTestDataBaseService( backupPort );

            try ( Transaction transaction = db.beginTx() )
            {
                db.index().forNodes( "created-no-commits" );
                transaction.success();
            }

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
            backup.full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            assertTrue( backup.isConsistent() );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private long getLastCommittedTx( String path, PageCache pageCache ) throws IOException
    {
        File neoStore = new File( path, MetaDataStore.DEFAULT_NAME );
        return MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
    }

    @Test
    public void backupEmptyIndex() throws Exception
    {
        int backupPort = PortAuthority.allocatePort();
        String key = "name";
        String value = "Neo";
        GraphDatabaseService db = getEmbeddedTestDataBaseService( backupPort );

        try
        {
            Index<Node> index;
            Node node;
            try ( Transaction tx = db.beginTx() )
            {
                index = db.index().forNodes( key );
                node = db.createNode();
                node.setProperty( key, value );
                tx.success();
            }
            OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort ).full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            assertEquals( DbRepresentation.of( db ), getDbRepresentation() );
            FileUtils.deleteDirectory( new File( backupPath.getPath() ) );
            backup = OnlineBackup.from( "127.0.0.1", backupPort ).full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            assertEquals( DbRepresentation.of( db ), getDbRepresentation() );

            try ( Transaction tx = db.beginTx() )
            {
                index.add( node, key, value );
                tx.success();
            }
            FileUtils.deleteDirectory( new File( backupPath.getPath() ) );
            backup = OnlineBackup.from( "127.0.0.1", backupPort ).full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            assertEquals( DbRepresentation.of( db ), getDbRepresentation() );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void backupMultipleSchemaIndexes() throws InterruptedException
    {
        // given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        AtomicBoolean end = new AtomicBoolean();
        int backupPort = PortAuthority.allocatePort();
        GraphDatabaseService db = getEmbeddedTestDataBaseService( backupPort );
        try
        {
            int numberOfIndexedLabels = 10;
            List<Label> indexedLabels = createIndexes( db, numberOfIndexedLabels );

            // start thread that continuously writes to indexes
            executorService.submit( () ->
            {
                while ( !end.get() )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        db.createNode( indexedLabels.get( random.nextInt( numberOfIndexedLabels ) ) ).setProperty( "prop", random.propertyValue() );
                        tx.success();
                    }
                }
            } );
            executorService.shutdown();

            // create backup
            OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort ).full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            end.set( true );
            executorService.awaitTermination( 1, TimeUnit.MINUTES );
        }
        finally
        {
            db.shutdown();
        }
    }

    private List<Label> createIndexes( GraphDatabaseService db, int indexCount )
    {
        ArrayList<Label> indexedLabels = new ArrayList<>( indexCount );
        for ( int i = 0; i < indexCount; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Label label = Label.label( "label" + i );
                indexedLabels.add( label );
                db.schema().indexFor( label ).on( "prop" ).create();
                tx.success();
            }
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
        return indexedLabels;
    }

    @Test
    public void shouldRetainFileLocksAfterFullBackupOnLiveDatabase()
    {
        int backupPort = PortAuthority.allocatePort();
        File sourcePath = testDir.directory( "serverdb-lock" );

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourcePath )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, "127.0.0.1:" + backupPort )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .newGraphDatabase();
        try
        {
            assertStoreIsLocked( sourcePath );
            OnlineBackup.from( "127.0.0.1", backupPort ).full( backupPath.getPath() );
            assertStoreIsLocked( sourcePath );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldIncrementallyBackupDenseNodes()
    {
        int backupPort = PortAuthority.allocatePort();
        GraphDatabaseService db = startGraphDatabase( serverPath, true, backupPort );
        try
        {
            createInitialDataset( db );

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
            backup.full( backupPath.getPath() );

            DbRepresentation representation = addLotsOfData( db );
            backup.incremental( backupPath.getPath() );
            assertEquals( representation, getDbRepresentation() );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldLeaveIdFilesAfterBackup() throws Exception
    {
        int backupPort = PortAuthority.allocatePort();
        GraphDatabaseService db = startGraphDatabase( serverPath, true, backupPort );
        try
        {
            createInitialDataset( db );

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
            backup.full( backupPath.getPath() );
            ensureStoresHaveIdFiles( backupPath );

            DbRepresentation representation = addLotsOfData( db );
            backup.incremental( backupPath.getPath() );
            assertEquals( representation, getDbRepresentation() );
            ensureStoresHaveIdFiles( backupPath );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void backupDatabaseWithCustomTransactionLogsLocation() throws IOException
    {
        int backupPort = PortAuthority.allocatePort();
        GraphDatabaseService db = startGraphDatabase( serverPath, true, backupPort, "customLogLocation" );
        try
        {
            createInitialDataset( db );

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1", backupPort );
            String backupStore = backupPath.getPath();
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( new File( backupStore ), fileSystemRule.get() ).build();

            backup.full( backupStore );
            assertThat( logFiles.logFiles(), Matchers.arrayWithSize( 1 ) );

            DbRepresentation representation = addLotsOfData( db );
            backup.incremental( backupStore );
            assertThat( logFiles.logFiles(), Matchers.arrayWithSize( 1 ) );

            assertEquals( representation, getDbRepresentation() );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void ensureStoresHaveIdFiles( File path ) throws IOException
    {
        for ( StoreFile file : StoreFile.values() )
        {
            if ( file.isRecordStore() )
            {
                File idFile = new File( path, file.fileName( StoreFileType.ID ) );
                assertTrue( "Missing id file " + idFile, idFile.exists() );
                assertTrue( "Id file " + idFile + " had 0 highId",
                        IdGeneratorImpl.readHighId( fileSystemRule.get(), idFile ) > 0 );
            }
        }
    }

    private DbRepresentation addLotsOfData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            int threshold = parseInt( dense_node_threshold.getDefaultValue() );
            for ( int i = 0; i < threshold * 2; i++ )
            {
                node.createRelationshipTo( db.createNode(), TEST );
            }
            tx.success();
        }
        return DbRepresentation.of( db );
    }

    private static void assertStoreIsLocked( File path )
    {
        try
        {
            new TestGraphDatabaseFactory().newEmbeddedDatabase( path ).shutdown();
            fail( "Could start up database in same process, store not locked" );
        }
        catch ( RuntimeException ex )
        {
            assertThat( ex.getCause().getCause(), instanceOf( StoreLockException.class ) );
        }
    }

    private static boolean checkLogFileExistence( String directory )
    {
        return Config.defaults( logs_directory, directory ).get( store_internal_log_path ).exists();
    }

    private long lastTxChecksumOf( File storeDir, PageCache pageCache ) throws IOException
    {
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        return MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
    }

    private ServerInterface startServer( File path, int backupPort )
    {
        ServerInterface server = new EmbeddedServer( path, "127.0.0.1:" + backupPort );
        server.awaitStarted();
        servers.add( server );
        return server;
    }

    private void shutdownServer( ServerInterface server )
    {
        server.shutdown();
        servers.remove( server );
    }

    private DbRepresentation addMoreData( File path )
    {
        GraphDatabaseService db = startGraphDatabase( path, false, null );
        DbRepresentation representation;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "backup", "Is great" );
            db.createNode().createRelationshipTo( node,
                    RelationshipType.withName( "LOVES" ) );
            tx.success();
        }
        finally
        {
            representation = DbRepresentation.of( db );
            db.shutdown();
        }
        return representation;
    }

    private GraphDatabaseService startGraphDatabase( File storeDir, boolean withOnlineBackup, Integer backupPort )
    {
        return startGraphDatabase( storeDir, withOnlineBackup, backupPort, "" );
    }

    private GraphDatabaseService startGraphDatabase( File storeDir, boolean withOnlineBackup, Integer backupPort,
            String logLocation )
    {
        GraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory()
        {
            @Override
            protected GraphDatabaseService newDatabase( File storeDir, Config config,
                    GraphDatabaseFacadeFactory.Dependencies dependencies )
            {
                Function<PlatformModule,EditionModule> factory =
                        platformModule -> new CommunityEditionModule( platformModule )
                        {

                            @Override
                            protected TransactionHeaderInformationFactory createHeaderInformationFactory()
                            {
                                return new TransactionHeaderInformationFactory.WithRandomBytes()
                                {
                                    @Override
                                    protected TransactionHeaderInformation createUsing( byte[] additionalHeader )
                                    {
                                        return new TransactionHeaderInformation( 1, 2, additionalHeader );
                                    }
                                };
                            }
                        };
                return new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, factory )
                        .newFacade( storeDir, config, dependencies );
            }
        };
        GraphDatabaseBuilder graphDatabaseBuilder = dbFactory.newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, String.valueOf( withOnlineBackup ) )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .setConfig( GraphDatabaseSettings.logical_logs_location, logLocation );

        if ( backupPort != null )
        {
            graphDatabaseBuilder.setConfig( OnlineBackupSettings.online_backup_server, "127.0.0.1:" + backupPort );
        }

        return graphDatabaseBuilder.newGraphDatabase();
    }

    private DbRepresentation createInitialDataSet( File path )
    {
        GraphDatabaseService db = startGraphDatabase( path, false, null );
        try
        {
            createInitialDataset( db );
            return DbRepresentation.of( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void createInitialDataset( GraphDatabaseService db )
    {
        // 4 transactions: THE transaction, "mykey" property key, "db-index" index, "KNOWS" rel type.
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( Label.label( "Me" ) );
            node.setProperty( "myKey", "myValue" );
            Index<Node> nodeIndex = db.index().forNodes( "db-index" );
            nodeIndex.add( node, "myKey", "myValue" );
            db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
    }

    private GraphDatabaseService getEmbeddedTestDataBaseService( int backupPort )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( serverPath )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, "127.0.0.1:" + backupPort )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .newGraphDatabase();
    }

    private DbRepresentation getDbRepresentation()
    {
        return DbRepresentation.of( backupPath, Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ) );
    }
}
