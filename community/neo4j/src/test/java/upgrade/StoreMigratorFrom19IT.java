/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package upgrade;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.LogDeserializer;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReader;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandReaderV1;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.util.Consumer;
import org.neo4j.kernel.impl.util.Cursor;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionLongToString;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.find19FormatStoreDirectory;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class StoreMigratorFrom19IT
{
    @Test
    public void shouldMigrate() throws IOException, ConsistencyCheckIncompleteException
    {
        // GIVEN
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR );
        upgrader.addParticipant( new StoreMigrator( monitor, fs ) );
        File legacyStoreDir = find19FormatStoreDirectory( storeDir.directory() );

        // WHEN
        upgrader.migrateIfNeeded( legacyStoreDir );

        // THEN
        assertEquals( 100, monitor.eventSize() );
        assertTrue( monitor.isStarted() );
        assertTrue( monitor.isFinished() );
        GraphDatabaseService database = cleanup.add(
                new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.absolutePath() )
        );

        try
        {
            verifyDatabaseContents( database );
        }
        finally
        {
            // CLEANUP
            database.shutdown();
        }

        NeoStore neoStore = cleanup.add( storeFactory.newNeoStore( storeFileName ) );
        verifyNeoStore( neoStore );
        neoStore.close();

        assertConsistentStore( storeDir.directory() );
    }

    @Test
    public void shouldMigrateCluster() throws Throwable
    {
        // Given
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR );
        upgrader.addParticipant( new StoreMigrator( monitor, fs ) );
        File legacyStoreDir = find19FormatStoreDirectory( storeDir.directory() );

        // When
        upgrader.migrateIfNeeded( legacyStoreDir );

        ManagedCluster cluster = cleanup.add( buildClusterWithMasterDirIn( legacyStoreDir ) );
        cluster.await( allSeesAllAsAvailable() );
        cluster.sync();

        // Then
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        verifySlaveContents( slave1 );
        verifySlaveContents( cluster.getAnySlave( slave1 ) );
        verifyDatabaseContents( cluster.getMaster() );
    }

    @Test
    public void shouldRewriteTransactionLogsDuringMigration() throws Throwable
    {
        // Given
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR );
        upgrader.addParticipant( new StoreMigrator( monitor, fs ) );
        File legacyStoreDir = find19FormatStoreDirectory( storeDir.directory() );

        // When
        upgrader.migrateIfNeeded( legacyStoreDir );

        // Then
        File[] transactionLogs = findAllMatchingFiles( legacyStoreDir, "nioneo_logical\\.log\\.v.*" );
        assertThat( transactionLogs, arrayWithSize( 2 ) );

        List<LogEntry> logEntries = readTransactionLogEntriesFrom( transactionLogs[1] );
        assertThat( logEntries, not( emptyCollectionOf( LogEntry.class ) ) );

        assertThat( logEntries.get( 0 ), instanceOf( LogEntry.Start.class ) );

        assertThat( logEntries.get( 1 ), instanceOf( LogEntry.Command.class ) );
        assertThat( ((LogEntry.Command) logEntries.get( 1 )).getXaCommand(), instanceOf( Command.NodeCommand.class ) );

        assertThat( ((LogEntry.Command) logEntries.get( logEntries.size() - 3 )).getXaCommand(),
                instanceOf( Command.PropertyCommand.class ) );

        assertThat( logEntries.get( logEntries.size() - 2 ), instanceOf( LogEntry.OnePhaseCommit.class ) );

        assertThat( logEntries.get( logEntries.size() - 1 ), instanceOf( LogEntry.Done.class ) );
    }

    @Test
    public void shouldRewriteLuceneLogsDuringMigration() throws Throwable
    {
        // Given
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR );
        upgrader.addParticipant( new StoreMigrator( monitor, fs ) );
        File legacyStoreDir = find19FormatStoreDirectory( storeDir.directory() );

        // When
        upgrader.migrateIfNeeded( legacyStoreDir );

        // Then
        File indexDir = new File( legacyStoreDir, "index" );
        File[] luceneLogs = findAllMatchingFiles( indexDir, "lucene\\.log\\.v.*" );
        assertThat( luceneLogs, arrayWithSize( 1 ) );

        List<LogEntry> logEntries = readLuceneLogEntriesFrom( luceneLogs[0] );
        assertThat( logEntries, hasSize( 23 ) );

        LogEntry log1 = logEntries.get( 0 );
        assertThat( log1, instanceOf( LogEntry.Start.class ) );
        assertThat( ((LogEntry.Start) log1).getMasterId(), equalTo( 2 ) );
        assertThat( ((LogEntry.Start) log1).getLocalId(), equalTo( 2 ) );

        LogEntry log2 = logEntries.get( 1 );
        assertThat( log2, instanceOf( LogEntry.Command.class ) );
        assertThat( ((LogEntry.Command) log2).getXaCommand().getClass().getSimpleName(), containsString( "Remove" ) );

        assertThat( logEntries.get( logEntries.size() - 3 ), instanceOf( LogEntry.Prepare.class ) );

        assertThat( logEntries.get( logEntries.size() - 2 ), instanceOf( LogEntry.TwoPhaseCommit.class ) );

        assertThat( logEntries.get( logEntries.size() - 1 ), instanceOf( LogEntry.Done.class ) );
    }

    private static File[] findAllMatchingFiles( File baseDir, String regex )
    {
        final Pattern pattern = Pattern.compile( regex );
        File[] files = baseDir.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String name )
            {
                return pattern.matcher( name ).matches();
            }
        } );
        Arrays.sort( files );
        return files;
    }

    private List<LogEntry> readTransactionLogEntriesFrom( File file ) throws IOException
    {
        return readAllLogEntries( file, new XaCommandReaderFactory()
        {
            @Override
            public XaCommandReader newInstance( byte logEntryVersion, ByteBuffer scratch )
            {
                return new PhysicalLogNeoXaCommandReaderV1( scratch );
            }
        } );
    }

    private List<LogEntry> readLuceneLogEntriesFrom( File file ) throws IOException
    {
        return readAllLogEntries( file, new LuceneDataSource.LuceneCommandReaderFactory( null, null ) );
    }

    private List<LogEntry> readAllLogEntries( File file, XaCommandReaderFactory cmdReaderFactory ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1000 );
        LogDeserializer logDeserializer = new LogDeserializer( buffer, cmdReaderFactory );

        final List<LogEntry> logs = new ArrayList<>();
        Consumer<LogEntry, IOException> consumer = new Consumer<LogEntry, IOException>()
        {
            @Override
            public boolean accept( LogEntry entry ) throws IOException
            {
                return logs.add( entry );
            }
        };

        try ( StoreChannel channel = fs.open( file, "r" );
              Cursor<LogEntry, IOException> cursor = logDeserializer.cursor( channel ) )
        {
            VersionAwareLogEntryReader.readLogHeader( buffer, channel, false );
            while ( cursor.next( consumer ) ) ;
        }

        return logs;
    }

    private static void verifyDatabaseContents( GraphDatabaseService db )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( db );
        verifyNumberOfNodesAndRelationships( verifier );
        verifier.verifyNodeIdsReused();
        verifier.verifyRelationshipIdsReused();
    }

    private static void verifySlaveContents( HighlyAvailableGraphDatabase haDb )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( haDb );
        verifyNumberOfNodesAndRelationships( verifier );
    }

    private static void verifyNumberOfNodesAndRelationships( DatabaseContentVerifier verifier )
    {
        verifier.verifyNodes( 110_000 );
        verifier.verifyRelationships( 99_900 );
    }

    private static void verifyNeoStore( NeoStore neoStore )
    {
        assertEquals( 1405267948320l, neoStore.getCreationTime() );
        assertEquals( -460827792522586619l, neoStore.getRandomNumber() );
        assertEquals( 15l, neoStore.getVersion() );
        assertEquals( ALL_STORES_VERSION, versionLongToString( neoStore.getStoreVersion() ) );
        assertEquals( 1004L + 3, neoStore.getLastCommittedTx() ); // prior verifications add 3 transactions
    }

    private ManagedCluster buildClusterWithMasterDirIn( final File legacyStoreDir ) throws Throwable
    {
        File haRootDir = new File( legacyStoreDir.getParentFile(), "ha-migration" );
        fs.deleteRecursively( haRootDir );

        ClusterManager clusterManager = new ClusterManager.Builder( haRootDir )
                .withStoreDirInitializer( new ClusterManager.StoreDirInitializer()
                {
                    @Override
                    public void initializeStoreDir( int serverId, File storeDir ) throws IOException
                    {
                        if ( serverId == 1 ) // Initialize dir only for master, others will copy store from it
                        {
                            FileUtils.copyRecursively( legacyStoreDir, storeDir );
                        }
                    }
                } )
                .withProvider( clusterOfSize( 3 ) )
                .build();

        clusterManager.start();

        cleanup.add( clusterManager );

        return clusterManager.getDefaultCluster();
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final ListAccumulatorMigrationProgressMonitor monitor = new ListAccumulatorMigrationProgressMonitor();
    private final IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
    private StoreFactory storeFactory;
    private File storeFileName;

    @Before
    public void setUp()
    {
        Config config = MigrationTestUtils.defaultConfig();
        storeFileName = new File( storeDir.absolutePath(), NeoStore.DEFAULT_NAME );
        storeFactory = new StoreFactory( config, idGeneratorFactory,
                new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, new DefaultTxHook() );
    }

    @Rule
    public final CleanupRule cleanup = new CleanupRule();

    @Rule
    public final TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( getClass() );
}
