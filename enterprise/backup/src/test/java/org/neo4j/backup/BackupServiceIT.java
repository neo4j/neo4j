/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.StoreFiles;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.BackupMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.index.impl.lucene.LuceneDataSource.DEFAULT_NAME;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;

public class BackupServiceIT
{
    private static final TargetDirectory target = TargetDirectory.forTest( BackupServiceIT.class );
    private static final String NODE_STORE = "neostore.nodestore.db";
    private static final String RELATIONSHIP_STORE = "neostore.relationshipstore.db";
    @Rule
    public TargetDirectory.TestDirectory testDirectory = target.testDirectory();

    public static final String BACKUP_HOST = "localhost";
    private FileSystemAbstraction fileSystem;

    private File storeDir;
    private File backupDir;

    public int backupPort = 8200;


    @Before
    public void setup() throws IOException
    {
        fileSystem = new DefaultFileSystemAbstraction();

        storeDir = new File( testDirectory.directory(), "store_dir" );
        fileSystem.deleteRecursively( storeDir );
        fileSystem.mkdir( storeDir );

        backupDir = new File( testDirectory.directory(), "backup_dir" );
        fileSystem.deleteRecursively( backupDir );

        backupPort = backupPort + 1;
    }

    @Test
    public void shouldThrowExceptionWhenDoingFullBackupOnADirectoryContainingANeoStore() throws Exception
    {
        // given
        fileSystem.mkdir( backupDir );
        fileSystem.create( new File( backupDir, NeoStore.DEFAULT_NAME ) ).close();

        try
        {
            // when
            new BackupService( fileSystem ).doFullBackup( "", 0, backupDir.getAbsolutePath(), true, new Config() );
        }
        catch ( RuntimeException ex )
        {
            // then
            assertThat( ex.getMessage(), containsString( "already contains a database" ) );
        }
    }

    @Test
    public void shouldCopyStoreFiles() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        File[] files = fileSystem.listFiles( backupDir );

        for ( final String fileName : StoreFiles.fileNames )
        {
            assertThat( files, hasFile( fileName ) );
        }

        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreAndLuceneTransactionInAnEmptyStore() throws IOException
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );

        assertNotNull( getLastMasterForCommittedTx( DEFAULT_DATA_SOURCE_NAME ) );
        assertNotNull( getLastMasterForCommittedTx( DEFAULT_NAME ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransaction() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );

        assertNotNull( getLastMasterForCommittedTx( DEFAULT_DATA_SOURCE_NAME ) );
    }

    @Test
    public void shouldFindValidPreviousCommittedTxIdInFirstNeoStoreLog() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );
        createAndIndexNode( db, 2 );
        createAndIndexNode( db, 3 );
        createAndIndexNode( db, 4 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        checkPreviousCommittedTxIdFromFirstLog( DEFAULT_DATA_SOURCE_NAME ); // neo store
        checkPreviousCommittedTxIdFromFirstLog( DEFAULT_NAME ); // lucene
    }

    @Test
    public void shouldFindTransactionLogContainingLastLuceneTransaction() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );

        assertNotNull( getLastMasterForCommittedTx( DEFAULT_NAME ) );
    }

    @Test
    public void shouldGiveHelpfulErrorMessageIfLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateLog( db );
        createAndIndexNode( db, 3 );
        rotateLog( db );
        createAndIndexNode( db, 3 );
        rotateLog( db );


        // when
        try
        {
            backupService.doIncrementalBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false );
            fail("Should have thrown exception.");
        }

        // Then
        catch(IncrementalBackupNotPossibleException e)
        {
            assertThat( e.getMessage(), equalTo("It's been too long since this backup was last updated, and it has " +
                    "fallen too far behind the database transaction stream for incremental backup to be possible. " +
                    "You need to perform a full backup at this point. You can modify this time interval by setting " +
                    "the '" + GraphDatabaseSettings.keep_logical_logs.name() + "' configuration on the database to a " +
                    "higher value.") );
        }
        db.shutdown();
    }

    @Test
    public void shouldFallbackToFullBackupIfIncrementalFailsAndExplicitlyAskedToDoThis() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateLog( db );
        createAndIndexNode( db, 3 );
        rotateLog( db );
        createAndIndexNode( db, 3 );
        rotateLog( db );


        // when
        backupService.doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, new Config(defaultBackupPortHostParams()));


        // Then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    @Test
    public void shouldDoFullBackupOnIncrementalFallbackToFullIfNoBackupFolderExists() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // when
        backupService.doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config(defaultBackupPortHostParams()));

        // then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    private void rotateLog( GraphDatabaseAPI db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource().rotateLogicalLog();
    }

    @Test
    public void shouldContainTransactionsThatHappenDuringBackupProcess() throws Throwable
    {
        // given
        Map<String, String> params = defaultBackupPortHostParams();
        params.put( OnlineBackupSettings.online_backup_enabled.name(), "false" );

        final List<String> storesThatHaveBeenStreamed = new ArrayList<String>(  );
        final CountDownLatch firstStoreFinishedStreaming = new CountDownLatch( 1 );
        final CountDownLatch transactionCommitted = new CountDownLatch( 1 );

        final GraphDatabaseAPI db = new EmbeddedGraphDatabase( storeDir.getAbsolutePath(), params );

        Config config = new Config( defaultBackupPortHostParams() );

        Monitors monitors = new Monitors();
        monitors.addMonitorListener( new BackupMonitor()
        {
            @Override
            public void startCopyingFiles()
            {

            }

            @Override
            public void finishedCopyingStoreFiles()
            {

            }

            @Override
            public void finishedRotatingLogicalLogs()
            {

            }

            @Override
            public void streamedFile( File storefile )
            {
                if ( neitherStoreHasBeenStreamed() )
                {
                    if ( storefile.getAbsolutePath().contains( NODE_STORE ) )
                    {
                        storesThatHaveBeenStreamed.add( NODE_STORE );
                        firstStoreFinishedStreaming.countDown();
                    }
                    else if ( storefile.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
                    {
                        storesThatHaveBeenStreamed.add(RELATIONSHIP_STORE);
                        firstStoreFinishedStreaming.countDown();
                    }
                }
            }

            private boolean neitherStoreHasBeenStreamed()
            {
                return storesThatHaveBeenStreamed.isEmpty();
            }

            @Override
            public void streamingFile( File storefile )
            {
                if ( storefile.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
                {
                    if ( streamedFirst( NODE_STORE ) )
                    {
                        try
                        {
                            transactionCommitted.await();
                        }
                        catch ( InterruptedException e )
                        {
                            e.printStackTrace();
                        }
                    }
                }
                else if ( storefile.getAbsolutePath().contains( NODE_STORE ) )
                {
                    if ( streamedFirst( RELATIONSHIP_STORE ) )
                    {
                        try
                        {
                            transactionCommitted.await();
                        }
                        catch ( InterruptedException e )
                        {
                            e.printStackTrace();
                        }
                    }
                }
            }

            private boolean streamedFirst( String store )
            {
                return !storesThatHaveBeenStreamed.isEmpty() && storesThatHaveBeenStreamed.get( 0 ).equals( store );
            }
        } );

        OnlineBackupKernelExtension backup = new OnlineBackupKernelExtension(
                config,
                db,
                db.getXaDataSourceManager(),
                db.getKernelPanicGenerator(),
                new DevNullLoggingService(),
                monitors );

        backup.start();

        // when
        BackupService backupService = new BackupService( fileSystem );

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    firstStoreFinishedStreaming.await();

                    Transaction tx = db.beginTx();
                    try
                    {
                        Node node1 = db.createNode();
                        Node node2 = db.createNode();
                        node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "foobydoo" ) );
                        tx.success();
                    }
                    finally
                    {
                        tx.finish();
                        db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource().getNeoStore().flushAll();
                        transactionCommitted.countDown();
                    }
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        } );

        BackupService.BackupOutcome backupOutcome = backupService.doFullBackup( BACKUP_HOST, backupPort,
                backupDir.getAbsolutePath(), true,
                new Config( params ) );

        backup.stop();
        executor.shutdown();
        executor.awaitTermination( 30, TimeUnit.SECONDS );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertTrue( backupOutcome.isConsistent() );
    }

    public static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);
    }

    private Map<String, String> defaultBackupPortHostParams()
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( OnlineBackupSettings.online_backup_server.name(), BACKUP_HOST + ":" + backupPort );
        return params;
    }

    private GraphDatabaseAPI createDb( File storeDir, Map<String, String> params )
    {
        return (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir.getPath() )
                .setConfig( params )
                .newGraphDatabase();
    }

    private void createAndIndexNode( GraphDatabaseService db, int i )
    {
        Transaction tx = db.beginTx();
        try
        {
            Index<Node> index = db.index().forNodes( "delete_me" );
            Node node = db.createNode();
            node.setProperty( "id", System.currentTimeMillis() + i );
            index.add( node, "delete", "me" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private BaseMatcher<File[]> hasFile( final String fileName )
    {
        return new BaseMatcher<File[]>()
        {
            @Override
            public boolean matches( Object o )
            {
                File[] files = (File[]) o;
                if ( files == null )
                {
                    return false;
                }
                for ( File file : files )
                {
                    if ( file.getAbsolutePath().contains( fileName ) )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( String.format( "[%s] in list of copied files", fileName ) );
            }
        };
    }

    private void checkPreviousCommittedTxIdFromFirstLog( String dataSourceName ) throws IOException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(
                backupDir.getAbsolutePath() );
        ReadableByteChannel logicalLog = null;
        try
        {
            XaDataSourceManager xaDataSourceManager = db.getDependencyResolver().resolveDependency(
                    XaDataSourceManager.class );
            XaDataSource dataSource = xaDataSourceManager.getXaDataSource( dataSourceName );
            logicalLog = dataSource.getLogicalLog( 0 );

            ByteBuffer buffer = ByteBuffer.allocate( 64 );
            long[] headerData = LogIoUtils.readLogHeader( buffer, logicalLog, true );

            long previousCommittedTxIdFromFirstLog = headerData[1];

            assertEquals( previousCommittedTxIdFromFirstLog, dataSource.getLastCommittedTxId() - 1 );
        }
        finally
        {
            db.shutdown();
            if ( logicalLog != null )
            {
                logicalLog.close();
            }
        }
    }

    private Pair<Integer,Long> getLastMasterForCommittedTx( String dataSourceName ) throws IOException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(
                backupDir.getAbsolutePath() );
        try
        {
            XaDataSourceManager xaDataSourceManager = db.getDependencyResolver().resolveDependency(
                    XaDataSourceManager.class );
            XaDataSource dataSource = xaDataSourceManager.getXaDataSource( dataSourceName );
            long lastCommittedTxId = dataSource.getLastCommittedTxId();
            return dataSource.getMasterForCommittedTx( lastCommittedTxId );
        }
        finally
        {
            db.shutdown();
        }
    }
}
