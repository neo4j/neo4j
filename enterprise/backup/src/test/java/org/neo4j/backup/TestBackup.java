/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.apache.commons.io.FileUtils;
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
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0_7;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.subprocess.SubProcess;

import static java.lang.Integer.parseInt;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;

@RunWith( Parameterized.class )
public class TestBackup
{
    private final TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( TestBackup.class );
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDir )
            .around( pageCacheRule )
            .around( SuppressOutput.suppressAll() );

    @Parameter
    public String recordFormatName;

    private File serverPath;
    private File otherServerPath;
    private File backupPath;
    private List<ServerInterface> servers;

    @Parameters( name = "{0}" )
    public static List<String> recordFormatNames()
    {
        return Arrays.asList( StandardV3_0_7.NAME, HighLimit.NAME );
    }

    @Before
    public void before() throws Exception
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
    public void makeSureFullFailsWhenDbExists() throws Exception
    {
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
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
    public void makeSureIncrementalFailsWhenNoDb() throws Exception
    {
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
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
            server = startServer( serverPath );
            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
            backup.full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            shutdownServer( server );
            server = null;
            PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );

            long firstChecksum = lastTxChecksumOf( serverPath, pageCache );
            assertEquals( firstChecksum, lastTxChecksumOf( backupPath, pageCache ) );

            addMoreData( serverPath );
            server = startServer( serverPath );
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
    public void fullThenIncremental() throws Exception
    {
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );

        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
        backup.full( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( initialDataSetRepresentation, getDbRepresentation() );
        shutdownServer( server );

        DbRepresentation furtherRepresentation = addMoreData( serverPath );
        server = startServer( serverPath );
        backup.incremental( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( furtherRepresentation, getDbRepresentation() );
        shutdownServer( server );
    }

    @Test
    public void makeSureNoLogFileRemains() throws Exception
    {
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );

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
        server = startServer( serverPath );
        backup.incremental( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertFalse( checkLogFileExistence( backupPath.getPath() ) );
        shutdownServer( server );
    }

    @Test
    public void makeSureStoreIdIsEnforced() throws Exception
    {
        // Create data set X on server A
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );

        // Grab initial backup from server A
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
        backup.full( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( initialDataSetRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );

        // Create data set X+Y on server B
        createInitialDataSet( otherServerPath );
        addMoreData( otherServerPath );
        server = startServer( otherServerPath );

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
        server = startServer( serverPath );
        backup.incremental( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( furtherRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );
    }

    @Test
    public void multipleIncrementals() throws Exception
    {
        GraphDatabaseService db = null;
        try
        {
            db = getEmbeddedTestDataBaseService();

            Index<Node> index;
            try ( Transaction tx = db.beginTx() )
            {
                index = db.index().forNodes( "yo" );
                index.add( db.createNode(), "justTo", "commitATx" );
                db.createNode();
                tx.success();
            }

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
            backup.full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );
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
    public void backupIndexWithNoCommits() throws Exception
    {
        GraphDatabaseService db = null;
        try
        {
            db = getEmbeddedTestDataBaseService();

            try ( Transaction transaction = db.beginTx() )
            {
                db.index().forNodes( "created-no-commits" );
                transaction.success();
            }

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
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
        String key = "name";
        String value = "Neo";
        GraphDatabaseService db = getEmbeddedTestDataBaseService();

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
            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" ).full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            assertEquals( DbRepresentation.of( db ), getDbRepresentation() );
            FileUtils.deleteDirectory( new File( backupPath.getPath() ) );
            backup = OnlineBackup.from( "127.0.0.1" ).full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            assertEquals( DbRepresentation.of( db ), getDbRepresentation() );

            try ( Transaction tx = db.beginTx() )
            {
                index.add( node, key, value );
                tx.success();
            }
            FileUtils.deleteDirectory( new File( backupPath.getPath() ) );
            backup = OnlineBackup.from( "127.0.0.1" ).full( backupPath.getPath() );
            assertTrue( "Should be consistent", backup.isConsistent() );
            assertEquals( DbRepresentation.of( db ), getDbRepresentation() );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldRetainFileLocksAfterFullBackupOnLiveDatabase() throws Exception
    {
        File sourcePath = testDir.directory( "serverdb-lock" );

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourcePath )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .newGraphDatabase();
        try
        {
            assertStoreIsLocked( sourcePath );
            OnlineBackup.from( "127.0.0.1" ).full( backupPath.getPath() );
            assertStoreIsLocked( sourcePath );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldIncrementallyBackupDenseNodes() throws Exception
    {
        GraphDatabaseService db = startGraphDatabase( serverPath, true );
        try
        {
            createInitialDataset( db );

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
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
        GraphDatabaseService db = startGraphDatabase( serverPath, true );
        try
        {
            createInitialDataset( db );

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
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

    private void ensureStoresHaveIdFiles( File path ) throws IOException
    {
        for ( StoreFile file : StoreFile.values() )
        {
            if ( file.isRecordStore() )
            {
                File idFile = new File( path, file.fileName( StoreFileType.ID ) );
                assertTrue( "Missing id file " + idFile, idFile.exists() );
                assertTrue( "Id file " + idFile + " had 0 highId",
                        IdGeneratorImpl.readHighId( new DefaultFileSystemAbstraction(), idFile ) > 0 );
            }
        }
    }

    private DbRepresentation addLotsOfData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            int threshold = parseInt( dense_node_threshold.getDefaultValue() );
            for ( int i = 0; i < threshold*2; i++ )
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

        StartupChecker proc = new LockProcess().start( path );
        try
        {
            assertFalse( "Could start up database in subprocess, store is not locked", proc.startupOk() );
        }
        finally
        {
            SubProcess.stop( proc );
        }
    }

    public interface StartupChecker
    {
        boolean startupOk();
    }

    @SuppressWarnings( "serial" )
    private static class LockProcess extends SubProcess<StartupChecker, File> implements StartupChecker
    {
        private volatile Object state;

        @Override
        public boolean startupOk()
        {
            Object result;
            do
            {
                result = state;
            }
            while ( result == null );
            return !( state instanceof Exception );
        }

        @Override
        protected void startup( File path ) throws Throwable
        {
            GraphDatabaseService db = null;
            try
            {
                db = new TestGraphDatabaseFactory().newEmbeddedDatabase( path );
            }
            catch ( RuntimeException ex )
            {
                if ( ex.getCause().getCause() instanceof StoreLockException )
                {
                    state = ex;
                    return;
                }
            }
            state = new Object();
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private static boolean checkLogFileExistence( String directory )
    {
        return new File( directory, StoreLogService.INTERNAL_LOG_NAME ).exists();
    }

    private long lastTxChecksumOf( File storeDir, PageCache pageCache ) throws IOException
    {
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        return MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
    }

    private ServerInterface startServer( File path ) throws Exception
    {
        ServerInterface server = new EmbeddedServer( path, "127.0.0.1:6362" );
        server.awaitStarted();
        servers.add( server );
        return server;
    }

    private void shutdownServer( ServerInterface server ) throws Exception
    {
        server.shutdown();
        servers.remove( server );
    }

    private DbRepresentation addMoreData( File path )
    {
        GraphDatabaseService db = startGraphDatabase( path, false );
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

    private GraphDatabaseService startGraphDatabase( File storeDir, boolean withOnlineBackup )
    {
        GraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory()
        {
            @Override
            protected GraphDatabaseService newDatabase( File storeDir, Map<String,String> config,
                    GraphDatabaseFacadeFactory.Dependencies dependencies )
            {
                return new CommunityFacadeFactory()
                {

                    @Override
                    protected EditionModule createEdition( PlatformModule platformModule )
                    {
                        return new CommunityEditionModule( platformModule )
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
                    }
                }.newFacade( storeDir, config, dependencies );
            }
        };
        return dbFactory.newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, String.valueOf( withOnlineBackup ) )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .newGraphDatabase();
    }

    private DbRepresentation createInitialDataSet( File path )
    {
        GraphDatabaseService db = startGraphDatabase( path, false );
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

    private GraphDatabaseService getEmbeddedTestDataBaseService()
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( serverPath )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .newGraphDatabase();
    }

    private DbRepresentation getDbRepresentation()
    {
        return DbRepresentation.of( backupPath );
    }
}
