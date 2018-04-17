/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.ClusterHelper;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.TestHelpers;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@RunWith( Parameterized.class )
public class OnlineBackupCommandCcIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 )
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( clusterRule );

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
                "--name=defaultport" ) );

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
        assertTrue( output.contains( "Start receiving index snapshot id 1" ) );
        assertTrue( output.contains( "Finished receiving index snapshot id 1" ) );
        assertTrue( output.contains( "Finished receiving index snapshots" ) );
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
