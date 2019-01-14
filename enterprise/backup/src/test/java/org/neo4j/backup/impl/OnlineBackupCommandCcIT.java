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
package org.neo4j.backup.impl;

import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import static org.junit.Assume.assumeFalse;
import static org.neo4j.backup.impl.OnlineBackupContextBuilder.ARG_NAME_FALLBACK_FULL;

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

    @Rule
    public final RuleChain ruleChain =
            RuleChain.outerRule( SuppressOutput.suppressAll() ).around( fileSystemRule ).around( testDirectory ).around( pageCacheRule ).around( clusterRule );

    private File backupDir;

    private List<Runnable> oneOffShutdownTasks;

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
        oneOffShutdownTasks = new ArrayList<>();
        backupDir = testDirectory.directory( "backups" );
    }

    @After
    public void performShutdownTasks()
    {
        oneOffShutdownTasks.forEach( Runnable::run );
    }

    @Test
    public void backupCanBePerformedOverCcWithCustomPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        Cluster cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", customAddress,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport", backupDir) );

        createSomeData( cluster );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", customAddress,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport", backupDir ) );
    }

    @Test
    public void dataIsInAUsableStateAfterBackup() throws Exception
    {
        // given database exists
        Cluster cluster = startCluster( recordFormat );

        // and the database has indexes
        ClusterHelper.createIndexes( cluster.getMemberWithAnyRole( Role.LEADER ).database() );

        // and the database is being populated
        AtomicBoolean populateDatabaseFlag = new AtomicBoolean( true );
        new Thread( () -> repeatedlyPopulateDatabase(  cluster, populateDatabaseFlag ) )
                .start(); // populate db with number properties etc.
        oneOffShutdownTasks.add( () -> populateDatabaseFlag.set( false ) ); // kill thread after test is complete

        // then backup is successful
        String address = TestHelpers.backupAddressCc( clusterLeader( cluster ).database() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir,
                "--name=defaultport" ) );
    }

    @Test
    public void backupCanBeOptionallySwitchedOnWithTheBackupConfig() throws Exception
    {
        // given a cluster with backup switched on
        int[] backupPorts = new int[]{PortAuthority.allocatePort(), PortAuthority.allocatePort(), PortAuthority.allocatePort()};
        String value = "localhost:%d";
        clusterRule = clusterRule.withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, "true" )
                .withInstanceCoreParam( OnlineBackupSettings.online_backup_server, i -> format( value, backupPorts[i] ) );
        Cluster cluster = startCluster( recordFormat );
        String customAddress = "localhost:" + backupPorts[0];

        // when a full backup is performed
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from=" + customAddress, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir,
                "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport", backupDir ) );

        // and an incremental backup is performed
        createSomeData( cluster );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from=" + customAddress, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir,
                "--name=defaultport",
                arg(ARG_NAME_FALLBACK_FULL, false) ) );

        // then the data matches
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport", backupDir ) );
    }

    @Test
    public void secondaryTransactionProtocolIsSwitchedOffCorrespondingBackupSetting() throws Exception
    {
        // given a cluster with backup switched off
        int[] backupPorts = new int[]{PortAuthority.allocatePort(), PortAuthority.allocatePort(), PortAuthority.allocatePort()};
        String value = "localhost:%d";
        clusterRule = clusterRule.withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, "false" )
                .withInstanceCoreParam( OnlineBackupSettings.online_backup_server, i -> format( value, backupPorts[i] ) );
        startCluster( recordFormat );
        String customAddress = "localhost:" + backupPorts[0];

        // then a full backup is impossible from the backup port
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "--from=" + customAddress, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir,
                "--name=defaultport" ) );
    }

    @Test
    public void backupDoesntDisplayExceptionWhenSuccessful() throws Exception
    {
        // given
        Cluster cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        // and
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outputStream = wrapWithNormalOutput( System.out, new PrintStream( byteArrayOutputStream ) );

        // and
        ByteArrayOutputStream byteArrayErrorStream = new ByteArrayOutputStream();
        PrintStream errorStream = wrapWithNormalOutput( System.err, new PrintStream( byteArrayErrorStream ) );

        // when
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( outputStream, errorStream,
                        "--from", customAddress,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );

        // then
        assertFalse( byteArrayErrorStream.toString().toLowerCase().contains( "exception" ) );
        assertFalse( byteArrayOutputStream.toString().toLowerCase().contains( "exception" ) );
    }

    @Test
    public void reportsProgress() throws Exception
    {
        // given
        Cluster cluster = startCluster( recordFormat );
        ClusterHelper.createIndexes( cluster.getMemberWithAnyRole( Role.LEADER ).database() );
        String customAddress = CausalClusteringTestHelpers.backupAddress( clusterLeader( cluster ).database() );

        // and
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outputStream = wrapWithNormalOutput( System.out, new PrintStream( byteArrayOutputStream ) );

        // and
        ByteArrayOutputStream byteArrayErrorStream = new ByteArrayOutputStream();
        PrintStream errorStream = wrapWithNormalOutput( System.err, new PrintStream( byteArrayErrorStream ) );

        // when
        String backupName = "reportsProgress_" + recordFormat;
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( outputStream, errorStream,
                        "--from", customAddress,
                        "--protocol=catchup",
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=" + backupName ) );

        // then
        String output = byteArrayOutputStream.toString();
        String location = Paths.get( backupDir.toString(), backupName ).toString();

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
        String address = TestHelpers.backupAddressCc( clusterLeader( cluster ).database() );

        String name = UUID.randomUUID().toString();
        File backupLocation = new File( backupDir, name );

        // when
        assertEquals( 0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir, "--name=" + name ) );

        // then
        assertFalse( "Store should not require recovery",
                new RecoveryRequiredChecker( fileSystemRule, pageCacheRule.getPageCache( fileSystemRule ), Config.defaults(),
                        new Monitors() ).isRecoveryRequiredAt( backupLocation ) );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        assertTrue( "Consistency check failed", new ConsistencyCheckService()
                .runFullConsistencyCheck( backupLocation, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false,
                        consistencyFlags )
                .isSuccessful() );
    }

    @Test
    public void incrementalBackupIsRecoveredAndConsistent() throws Exception
    {
        // given database exists with data
        Cluster cluster = startCluster( recordFormat );
        createSomeData( cluster );
        String address = TestHelpers.backupAddressCc( clusterLeader( cluster ).database() );

        String name = UUID.randomUUID().toString();
        File backupLocation = new File( backupDir, name );

        // when
        assertEquals( 0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir, "--name=" + name ) );

        // and
        createSomeData( cluster );
        assertEquals( 0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir, "--name=" + name,
                        arg( ARG_NAME_FALLBACK_FULL, false ) ) );

        // then
        assertFalse( "Store should not require recovery",
                new RecoveryRequiredChecker( fileSystemRule, pageCacheRule.getPageCache( fileSystemRule ), Config.defaults(),
                        new Monitors() ).isRecoveryRequiredAt( backupLocation ) );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        assertTrue( "Consistency check failed", new ConsistencyCheckService()
                .runFullConsistencyCheck( backupLocation, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false,
                        consistencyFlags )
                .isSuccessful() );
    }

    @Test
    public void onlyTheLatestTransactionIsKeptAfterIncrementalBackup() throws Exception
    {
        // given database exists with data
        Cluster cluster = startCluster( recordFormat );
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
                "--cc-report-dir=" + backupDir,
                "--backup-dir=" + backupDir,
                "--additional-config=" + configOverrideFile,
                "--name=" + backupName ) );

        // and the database contains a few more transactions
        transactions1M( cluster );
        transactions1M( cluster ); // rotation, second tx log file

        // when we perform an incremental backup
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                "--from", address,
                "--cc-report-dir=" + backupDir,
                "--backup-dir=" + backupDir,
                "--additional-config=" + configOverrideFile,
                "--name=" + backupName,
                arg(ARG_NAME_FALLBACK_FULL, false) ) );

        // then there has been a rotation
        BackupTransactionLogFilesHelper backupTransactionLogFilesHelper = new BackupTransactionLogFilesHelper();
        LogFiles logFiles = backupTransactionLogFilesHelper.readLogFiles( backupDir.toPath().resolve( backupName ).toFile() );
        long highestTxIdInLogFiles = logFiles.getHighestLogVersion();
        assertEquals( 2, highestTxIdInLogFiles );

        // and the original log has not been removed since the transactions are applied at start
        long lowestTxIdInLogFiles = logFiles.getLowestLogVersion();
        assertEquals( 0, lowestTxIdInLogFiles );
    }

    @Test
    public void ipv6Enabled() throws Exception
    {
        // given
        Cluster cluster = startIpv6Cluster();
        try
        {
            assertNotNull( DbRepresentation.of( clusterDatabase( cluster ) ) );
            int port = clusterLeader( cluster ).config().get( CausalClusteringSettings.transaction_listen_address ).getPort();
            String customAddress = String.format( "[%s]:%d", IpFamily.IPV6.localhostAddress(), port );
            String backupName = "backup_" + recordFormat;

            // when full backup
            assertEquals( 0, runBackupToolFromSameJvmToGetExitCode(
                    "--from", customAddress,
                    "--protocol=catchup",
                    "--cc-report-dir=" + backupDir,
                    "--backup-dir=" + backupDir,
                    "--name=" + backupName ) );

            // and
            createSomeData( cluster );

            // and incremental backup
            assertEquals( 0, runBackupToolFromSameJvmToGetExitCode(
                    "--from", customAddress,
                    "--protocol=catchup",
                    "--cc-report-dir=" + backupDir,
                    "--name=" + backupName, "--backup-dir=" + backupDir,
                    arg(ARG_NAME_FALLBACK_FULL, false) ) );

            // then
            assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( backupName, backupDir ) );
        }
        finally
        {
            cluster.shutdown();
        }
    }

    @Test
    public void backupRenamesWork() throws Exception
    {
        // given a prexisting backup from a different store
        String backupName = "preexistingBackup_" + recordFormat;
        Cluster cluster = startCluster( recordFormat );
        String firstBackupAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                "--from", firstBackupAddress,
                "--cc-report-dir=" + backupDir,
                "--backup-dir=" + backupDir,
                "--name=" + backupName ) );
        DbRepresentation firstDatabaseRepresentation = DbRepresentation.of( clusterLeader( cluster ).database() );

        // and a different database
        Cluster cluster2 = startCluster2( recordFormat );
        try
        {

            DbRepresentation secondDatabaseRepresentation = DbRepresentation.of( clusterLeader( cluster2 ).database() );
            assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
            String secondBackupAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster2 ).database() );

            // when backup is performed
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                    "--from", secondBackupAddress,
                    "--cc-report-dir=" + backupDir,
                    "--backup-dir=" + backupDir,
                    "--name=" + backupName ) );
            cluster2.shutdown();

            // then the new backup has the correct name
            assertEquals( secondDatabaseRepresentation, getBackupDbRepresentation( backupName, backupDir ) );

            // and the old backup is in a renamed location
            assertEquals( firstDatabaseRepresentation, getBackupDbRepresentation( backupName + ".err.0", backupDir ) );

            // and the data isn't equal (sanity check)
            assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
        }
        finally
        {
            cluster2.shutdown();
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

    private void repeatedlyPopulateDatabase( Cluster cluster, AtomicBoolean continueFlagReference )
    {
        while ( continueFlagReference.get() )
        {
            createSomeData( cluster );
        }
    }

    public static CoreGraphDatabase clusterDatabase( Cluster cluster )
    {
        return clusterLeader( cluster ).database();
    }

    private Cluster startCluster( String recordFormat ) throws Exception
    {
        ClusterRule clusterRule = this.clusterRule.withSharedCoreParam( GraphDatabaseSettings.record_format, recordFormat )
                .withSharedReadReplicaParam( GraphDatabaseSettings.record_format, recordFormat );
        Cluster cluster = clusterRule.startCluster();
        createSomeData( cluster );
        return cluster;
    }

    private Cluster startIpv6Cluster() throws ExecutionException, InterruptedException
    {
        DiscoveryServiceFactory discoveryServiceFactory = new SharedDiscoveryServiceFactory();
        File parentDir = testDirectory.directory( "ipv6_cluster" );
        Map<String,String> coreParams = new HashMap<>();
        coreParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();

        Map<String,String> readReplicaParams = new HashMap<>();
        readReplicaParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();

        Cluster cluster = new Cluster( parentDir, 3, 3, discoveryServiceFactory, coreParams, instanceCoreParams, readReplicaParams, instanceReadReplicaParams,
                recordFormat, IpFamily.IPV6, false );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private Cluster startCluster2( String recordFormat ) throws ExecutionException, InterruptedException
    {
        Map<String,String> sharedParams = new HashMap<>(  );
        sharedParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Cluster cluster =
                new Cluster( testDirectory.directory( "cluster-b_" + recordFormat ), 3, 0, new SharedDiscoveryServiceFactory(), sharedParams, emptyMap(),
                sharedParams, emptyMap(),
                recordFormat, IpFamily.IPV4, false );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private static void transactions1M( Cluster cluster ) throws Exception
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

    public static DbRepresentation createSomeData( Cluster cluster )
    {
        try
        {
            cluster.coreTx( ClusterHelper::createSomeData );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        return DbRepresentation.of( clusterLeader( cluster ).database() );
    }

    private static CoreClusterMember clusterLeader( Cluster cluster )
    {
        return cluster.getMemberWithRole( Role.LEADER );
    }

    public static DbRepresentation getBackupDbRepresentation( String name, File backupDir )
    {
        Config config = Config.defaults();
        config.augment( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        return DbRepresentation.of( new File( backupDir, name ), config );
    }

    private int runBackupToolFromOtherJvmToGetExitCode( PrintStream outputStream, PrintStream errorStream, String... args ) throws Exception
    {
        return TestHelpers.runBackupToolFromOtherJvmToGetExitCode( testDirectory.absolutePath(), outputStream, errorStream, false, args );
    }

    private int runBackupToolFromOtherJvmToGetExitCode( String... args ) throws Exception
    {
        return TestHelpers.runBackupToolFromOtherJvmToGetExitCode( testDirectory.absolutePath(), args );
    }

    /**
     * This unused method is used for debugging, so don't remove
     */
    private int runBackupToolFromSameJvmToGetExitCode( String... args ) throws Exception
    {
        return new OnlineBackupCommandBuilder().withRawArgs( args ).backup( testDirectory.absolutePath(), testDirectory.absolutePath().getName() )
               ? 0 : 1;
    }
}
