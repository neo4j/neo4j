/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.backup.impl;

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
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.stream.LongStream;

import org.neo4j.causalclustering.ClusterHelper;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.EnterpriseCluster;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;
import org.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.util.TestHelpers;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.backup.impl.OnlineBackupContextFactory.ARG_NAME_FALLBACK_FULL;
import static org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_server;

@RunWith( Parameterized.class )
public class OnlineBackupCommandCcIT
{
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 )
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" );

    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final RuleChain ruleChain =
            RuleChain.outerRule( suppressOutput ).around( fileSystemRule ).around( testDirectory ).around( pageCacheRule ).around( clusterRule );

    private static final String DATABASE_NAME = "defaultport";
    private File backupDatabaseDir;
    private File backupStoreDir;

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    @Before
    public void initialiseBackupDirectory()
    {
        backupStoreDir = testDirectory.directory( "backupStore" );
        backupDatabaseDir = new File( backupStoreDir, DATABASE_NAME );
        backupDatabaseDir.mkdirs();
    }

    @Test
    public void backupCanBePerformedOverCcWithCustomPort() throws Exception
    {
        Cluster<?> cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        assertEquals( 0, runBackupOtherJvm( customAddress, DATABASE_NAME ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( DATABASE_NAME, backupStoreDir ) );

        createSomeData( cluster );
        assertEquals( 0, runBackupOtherJvm( customAddress, DATABASE_NAME ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( DATABASE_NAME, backupStoreDir ) );
    }

    @Test
    public void dataIsInAUsableStateAfterBackup() throws Exception
    {
        // given database exists
        Cluster<?> cluster = startCluster( recordFormat );

        // and the database has indexes
        ClusterHelper.createIndexes( cluster.getMemberWithAnyRole( Role.LEADER ).database() );

        // and the database is being populated
        AtomicBoolean populateDatabaseFlag = new AtomicBoolean( true );
        Thread thread = new Thread( () -> repeatedlyPopulateDatabase( cluster, populateDatabaseFlag ) );
        thread.start(); // populate db with number properties etc.
        try
        {
            // then backup is successful
            String address = cluster.awaitLeader().config().get( online_backup_server ).toString();
            assertEquals( 0, runBackupOtherJvm( address, DATABASE_NAME ) );
        }
        finally
        {
            populateDatabaseFlag.set( false );
            thread.join();
        }
    }

    @Test
    public void backupCanBeOptionallySwitchedOnWithTheBackupConfig() throws Exception
    {
        // given a cluster with backup switched on
        int[] backupPorts = new int[]{PortAuthority.allocatePort(), PortAuthority.allocatePort(), PortAuthority.allocatePort()};
        String value = "localhost:%d";
        clusterRule = clusterRule.withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, "true" )
                .withInstanceCoreParam( online_backup_server, i -> format( value, backupPorts[i] ) );
        Cluster<?> cluster = startCluster( recordFormat );
        String customAddress = "localhost:" + backupPorts[0];

        // when a full backup is performed
        assertEquals( 0, runBackupOtherJvm( customAddress, DATABASE_NAME ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( DATABASE_NAME, backupStoreDir ) );

        // and an incremental backup is performed
        createSomeData( cluster );
        assertEquals( 0, runBackupOtherJvm( customAddress, DATABASE_NAME ) );
        assertEquals( 0,
                runBackupToolFromOtherJvmToGetExitCode( "--from=" + customAddress, "--cc-report-dir=" + backupStoreDir, "--backup-dir=" + backupStoreDir,
                        "--name=defaultport", arg( ARG_NAME_FALLBACK_FULL, false ) ) );

        // then the data matches
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( DATABASE_NAME, backupStoreDir ) );
    }

    @Test
    public void secondaryTransactionProtocolIsSwitchedOffCorrespondingBackupSetting() throws Exception
    {
        // given a cluster with backup switched off
        int[] backupPorts = new int[]{PortAuthority.allocatePort(), PortAuthority.allocatePort(), PortAuthority.allocatePort()};
        String value = "localhost:%d";
        clusterRule = clusterRule.withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, "false" )
                .withInstanceCoreParam( online_backup_server, i -> format( value, backupPorts[i] ) );
        startCluster( recordFormat );
        String customAddress = "localhost:" + backupPorts[0];

        // then a full backup is impossible from the backup port
        assertEquals( 1, runBackupOtherJvm( customAddress, DATABASE_NAME ) );
    }

    @Test
    public void backupDoesntDisplayExceptionWhenSuccessful() throws Exception
    {
        // given
        Cluster<?> cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        // when
        assertEquals( 0, runBackupOtherJvm( customAddress, DATABASE_NAME ) );

        // then
        assertFalse( suppressOutput.getErrorVoice().toString().toLowerCase().contains( "exception" ) );
        assertFalse( suppressOutput.getOutputVoice().toString().toLowerCase().contains( "exception" ) );
    }

    @Test
    public void reportsProgress() throws Exception
    {
        // given
        Cluster<?> cluster = startCluster( recordFormat );
        ClusterHelper.createIndexes( cluster.getMemberWithAnyRole( Role.LEADER ).database() );
        String customAddress = CausalClusteringTestHelpers.backupAddress( clusterLeader( cluster ).database() );

        // when
        final String backupName = "reportsProgress_" + recordFormat;
        assertEquals( 0, runBackupOtherJvm( customAddress, backupName ));

        // then
        String output = suppressOutput.getOutputVoice().toString();
        String location = Paths.get( backupStoreDir.toString(), backupName ).toString();

        assertTrue( output.contains( "Start receiving store files" ) );
        assertTrue( output.contains( "Finish receiving store files" ) );
        String tested = Paths.get( location, "neostore.nodestore.db.labels" ).toString();
        assertTrue( tested, output.contains( format( "Start receiving store file %s", tested ) ) );
        assertTrue( tested, output.contains( format( "Finish receiving store file %s", tested ) ) );
        assertTrue( output.contains( "Start receiving transactions from " ) );
        assertTrue( output.contains( "Finish receiving transactions at " ) );
        assertTrue( output.contains( "Start receiving index snapshots" ) );
        assertTrue( output.contains( "Finished receiving index snapshots" ) );
    }

    @Test
    public void fullBackupIsRecoveredAndConsistent() throws Exception
    {
        // given database exists with data
        Cluster cluster = startCluster( recordFormat );
        createSomeData( cluster );
        String address = cluster.awaitLeader().config().get( online_backup_server ).toString();

        String name = UUID.randomUUID().toString();
        File backupLocation = new File( backupStoreDir, name );
        DatabaseLayout backupLayout = DatabaseLayout.of( backupLocation );

        // when
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--cc-report-dir=" + backupStoreDir, "--backup-dir=" + backupStoreDir,
                "--name=" + name ) );

        // then
        assertFalse( "Store should not require recovery",
                new RecoveryRequiredChecker( fileSystemRule, pageCacheRule.getPageCache( fileSystemRule ), Config.defaults(),
                        new Monitors() ).isRecoveryRequiredAt( backupLayout ) );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        assertTrue( "Consistency check failed", new ConsistencyCheckService()
                .runFullConsistencyCheck( backupLayout, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false, consistencyFlags )
                .isSuccessful() );
    }

    @Test
    public void incrementalBackupIsRecoveredAndConsistent() throws Exception
    {
        // given database exists with data
        Cluster cluster = startCluster( recordFormat );
        createSomeData( cluster );
        String address = cluster.awaitLeader().config().get( online_backup_server ).toString();

        String name = UUID.randomUUID().toString();
        File backupLocation = new File( backupStoreDir, name );
        DatabaseLayout backupLayout = DatabaseLayout.of( backupLocation );

        // when
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--cc-report-dir=" + backupStoreDir, "--backup-dir=" + backupStoreDir,
                "--name=" + name ) );

        // and
        createSomeData( cluster );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--cc-report-dir=" + backupStoreDir, "--backup-dir=" + backupStoreDir,
                "--name=" + name, arg( ARG_NAME_FALLBACK_FULL, false ) ) );

        // then
        assertFalse( "Store should not require recovery",
                new RecoveryRequiredChecker( fileSystemRule, pageCacheRule.getPageCache( fileSystemRule ), Config.defaults(),
                        new Monitors() ).isRecoveryRequiredAt( backupLayout ) );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        assertTrue( "Consistency check failed", new ConsistencyCheckService()
                .runFullConsistencyCheck( backupLayout, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false, consistencyFlags )
                .isSuccessful() );
    }

    @Test
    public void onlyTheLatestTransactionIsKeptAfterIncrementalBackup() throws Exception
    {
        // given database exists with data
        Cluster<?> cluster = startCluster( recordFormat );
        createSomeData( cluster );

        // and backup client is told to rotate conveniently
        Config config = Config
                .builder()
                .withSetting( GraphDatabaseSettings.logical_log_rotation_threshold, "1m" )
                .build();
        File configOverrideFile = testDirectory.file( "neo4j-backup.conf" );
        OnlineBackupCommandBuilder.writeConfigToFile( config, configOverrideFile );

        // and we have a full backup
        final String backupName = "backupName" + recordFormat;
        String address = CausalClusteringTestHelpers.backupAddress( clusterLeader( cluster ).database() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                "--from", address,
                "--cc-report-dir=" + backupStoreDir,
                "--backup-dir=" + backupStoreDir,
                "--additional-config=" + configOverrideFile,
                "--name=" + backupName ) );

        // and the database contains a few more transactions
        transactions1M( cluster );
        transactions1M( cluster ); // rotation, second tx log file

        // when we perform an incremental backup
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                "--from", address,
                "--cc-report-dir=" + backupStoreDir,
                "--backup-dir=" + backupStoreDir, "--additional-config=" + configOverrideFile, "--name=" + backupName, arg( ARG_NAME_FALLBACK_FULL, false ) ) );

        // then there has been a rotation
        LogFiles logFiles = BackupTransactionLogFilesHelper.readLogFiles( DatabaseLayout.of( new File( backupStoreDir, backupName ) ) );
        long highestTxIdInLogFiles = logFiles.getHighestLogVersion();
        assertEquals( 2, highestTxIdInLogFiles );

        // and the original log has not been removed since the transactions are applied at start
        long lowestTxIdInLogFiles = logFiles.getLowestLogVersion();
        assertEquals( 0, lowestTxIdInLogFiles );
    }

    @Test
    public void backupRenamesWork() throws Exception
    {
        // given a prexisting backup from a different store
        String backupName = "preexistingBackup_" + recordFormat;
        Cluster<?> cluster = startCluster( recordFormat );
        String firstBackupAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        assertEquals( 0, runBackupOtherJvm( firstBackupAddress, backupName ) );
        DbRepresentation firstDatabaseRepresentation = DbRepresentation.of( clusterLeader( cluster ).database() );

        // and a different database
        Cluster<?> cluster2 = startCluster2( recordFormat );
        DbRepresentation secondDatabaseRepresentation = DbRepresentation.of( clusterLeader( cluster2 ).database() );
        assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
        String secondBackupAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster2 ).database() );

        // when backup is performed
        assertEquals( 0, runBackupOtherJvm( secondBackupAddress, backupName ) );
        cluster2.shutdown();

        // then the new backup has the correct name
        assertEquals( secondDatabaseRepresentation, getBackupDbRepresentation( backupName, backupStoreDir ) );

        // and the old backup is in a renamed location
        assertEquals( firstDatabaseRepresentation, getBackupDbRepresentation( backupName + ".err.0", backupStoreDir ) );

        // and the data isn't equal (sanity check)
        assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
    }

    private int runBackupOtherJvm( String customAddress, String databaseName ) throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode(
                "--from", customAddress,
                "--cc-report-dir=" + backupStoreDir,
                "--backup-dir=" + backupStoreDir,
                "--name=" + databaseName );
    }

    @Test
    public void ipv6Enabled() throws Exception
    {
        // given
        Cluster<?> cluster = startIpv6Cluster();
        try
        {
            assertNotNull( DbRepresentation.of( clusterDatabase( cluster ) ) );
            int port = clusterLeader( cluster ).config().get( CausalClusteringSettings.transaction_listen_address ).getPort();
            String customAddress = String.format( "[%s]:%d", IpFamily.IPV6.localhostAddress(), port );
            String backupName = "backup_" + recordFormat;

            // when full backup
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                    "--from", customAddress,
                    "--protocol=catchup",
                    "--cc-report-dir=" + backupStoreDir,
                    "--backup-dir=" + backupStoreDir,
                    "--name=" + backupName ) );

            // and
            createSomeData( cluster );

            // and incremental backup
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                    "--from", customAddress,
                    "--protocol=catchup",
                    "--cc-report-dir=" + backupStoreDir, "--backup-dir=" + backupStoreDir, "--name=" + backupName, arg( ARG_NAME_FALLBACK_FULL, false ) ) );

            // then
            assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( backupName, backupStoreDir ) );
        }
        finally
        {
            cluster.shutdown();
        }
    }

    static String arg( String key, Object value )
    {
        return "--" + key + "=" + value;
    }

    static PrintStream wrapWithNormalOutput( PrintStream normalOutput, PrintStream nullAbleOutput )
    {
        if ( nullAbleOutput == null )
        {
            return normalOutput;
        }
        return duplexPrintStream( normalOutput, nullAbleOutput );
    }

    private static PrintStream duplexPrintStream( PrintStream first, PrintStream second )
    {
        return new PrintStream( first )
        {

            @Override
            public void write( int i )
            {
                super.write( i );
                second.write( i );
            }

            @Override
            public void write( byte[] bytes, int i, int i1 )
            {
                super.write( bytes, i, i1 );
                second.write( bytes, i, i1 );
            }

            @Override
            public void write( byte[] bytes ) throws IOException
            {
                super.write( bytes );
                second.write( bytes );
            }

            @Override
            public void flush()
            {
                super.flush();
                second.flush();
            }

            @Override
            public void close()
            {
                super.close();
                second.close();
            }
        };
    }

    private static void repeatedlyPopulateDatabase( Cluster<?> cluster, AtomicBoolean continueFlagReference )
    {
        while ( continueFlagReference.get() )
        {
            createSomeData( cluster );
        }
    }

    public static CoreGraphDatabase clusterDatabase( Cluster<?> cluster )
    {
        return clusterLeader( cluster ).database();
    }

    private Cluster<?> startCluster( String recordFormat ) throws Exception
    {
        ClusterRule clusterRule = this.clusterRule.withSharedCoreParam( GraphDatabaseSettings.record_format, recordFormat )
                .withSharedReadReplicaParam( GraphDatabaseSettings.record_format, recordFormat );
        Cluster<?> cluster = clusterRule.startCluster();
        createSomeData( cluster );
        return cluster;
    }

    private Cluster<?> startIpv6Cluster() throws ExecutionException, InterruptedException
    {
        DiscoveryServiceFactory discoveryServiceFactory = new SharedDiscoveryServiceFactory();
        File parentDir = testDirectory.directory( "ipv6_cluster" );
        Map<String,String> coreParams = new HashMap<>();
        coreParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();

        Map<String,String> readReplicaParams = new HashMap<>();
        readReplicaParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();

        Cluster<?> cluster = new EnterpriseCluster( parentDir, 3, 3, discoveryServiceFactory, coreParams, instanceCoreParams, readReplicaParams,
                instanceReadReplicaParams, recordFormat, IpFamily.IPV6, false );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private Cluster<?> startCluster2( String recordFormat ) throws ExecutionException, InterruptedException
    {
        Map<String,String> sharedParams = new HashMap<>(  );
        sharedParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Cluster<?> cluster =
                new EnterpriseCluster( testDirectory.directory( "cluster-b_" + recordFormat ), 3, 0, new SharedDiscoveryServiceFactory(),
                        sharedParams, emptyMap(), sharedParams, emptyMap(),
                recordFormat, IpFamily.IPV4, false );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private static void transactions1M( Cluster<?> cluster ) throws Exception
    {
        int numberOfTransactions = 500;
        long sizeOfTransaction = (ByteUnit.mebiBytes( 1 ) / numberOfTransactions) + 1;
        for ( int txId = 0; txId < numberOfTransactions; txId++ )
        {
            cluster.coreTx( ( coreGraphDatabase, transaction ) ->
            {
                Node node = coreGraphDatabase.createNode();
                String longString = LongStream.range( 0, sizeOfTransaction ).map( l -> l % 10 ).mapToObj( Long::toString ).collect( joining( "" ) );
                node.setProperty( "name", longString );
                coreGraphDatabase.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
                transaction.success();
            } );
        }
    }

    public static void createSomeData( Cluster<?> cluster )
    {
        try
        {
            cluster.coreTx( ClusterHelper::createSomeData );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private static CoreClusterMember clusterLeader( Cluster<?> cluster )
    {
        return cluster.getMemberWithRole( Role.LEADER );
    }

    public static DbRepresentation getBackupDbRepresentation( String name, File storeDir )
    {
        Config config = Config.defaults();
        config.augment( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        return DbRepresentation.of( DatabaseLayout.of( storeDir, name ).databaseDirectory(), config );
    }

    private int runBackupToolFromOtherJvmToGetExitCode( String... args ) throws Exception
    {
        return TestHelpers.runBackupToolFromOtherJvmToGetExitCode( testDirectory.absolutePath(), args );
    }

    private int runBackupToolFromSameJvm( String... args ) throws Exception
    {
        return runBackupToolFromSameJvmToGetExitCode( testDirectory.absolutePath(), testDirectory.absolutePath().getName(), args );
    }

    /**
     * This unused method is used for debugging, so don't remove
     */
    private static int runBackupToolFromSameJvmToGetExitCode( File backupDir, String backupName, String... args ) throws Exception
    {
        return new OnlineBackupCommandBuilder().withRawArgs( args ).backup( backupDir, backupName ) ? 0 : 1;
    }
}
