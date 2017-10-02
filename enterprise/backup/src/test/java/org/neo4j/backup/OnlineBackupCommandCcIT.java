/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.TestHelpers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeFalse;

@RunWith( Parameterized.class )
public class OnlineBackupCommandCcIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 )
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( clusterRule );

    private static final String ip = "127.0.0.1";

    private File backupDir;

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
        backupDir = testDirectory.directory( "backups" );
    }

    @Test
    public void backupCanBePerformedOverCcWithDefaultPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        Cluster cluster = startCluster( null, false, null, recordFormat );
        createSomeData( clusterDatabase( cluster ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", ip,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport" ) );

        createSomeData( clusterDatabase( cluster ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", ip,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport" ) );
    }

    @Test
    public void backupCanBePerformedOverCcWithCustomPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );
        String customAddress = ip + ":" + 19911;

        Cluster cluster = startCluster( 19911, false, null, recordFormat );
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", customAddress,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport" ) );

        createSomeData( clusterDatabase( cluster ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", customAddress,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport" ) );
    }

    @Test
    public void backupCanNotBePerformedOverBackupProtocol() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        startCluster( 9999, true, null, recordFormat );
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "--from", ip,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
    }

    private static CoreGraphDatabase clusterDatabase( Cluster cluster )
    {
        return clusterLeader( cluster ).database();
    }

    private Cluster startCluster( @Nullable Integer transactionPort, boolean backupProtocolEnabled, @Nullable Integer backupProtocolPort, String recordFormat ) throws Exception
    {
        Optional<Integer> optionalTxPort = Optional.ofNullable( transactionPort );
        Optional<Integer> optionalBackupProtocolPort = Optional.ofNullable( backupProtocolPort );
        ClusterRule clusterRule = this.clusterRule
                .withSharedCoreParam( GraphDatabaseSettings.record_format, recordFormat )
                .withSharedReadReplicaParam( GraphDatabaseSettings.record_format, recordFormat );
        if ( optionalTxPort.isPresent() )
        {
            clusterRule = clusterRule
                    .withInstanceCoreParam( CausalClusteringSettings.transaction_listen_address, addressWithIncrementingPortPerInstance( optionalTxPort.get() ) );
        }
        if ( !backupProtocolEnabled )
        {
            clusterRule = clusterRule
                    .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                    .withSharedReadReplicaParam( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        }
        if ( optionalBackupProtocolPort.isPresent() )
        {
            int numberOfCores = 3;
            clusterRule = clusterRule
                    .withInstanceCoreParam( OnlineBackupSettings.online_backup_server,
                            addressWithIncrementingPortPerInstance( optionalBackupProtocolPort.get() ) )
                    .withInstanceReadReplicaParam( OnlineBackupSettings.online_backup_server,
                            addressWithIncrementingPortPerInstance( optionalBackupProtocolPort.get() + numberOfCores ) );
        }
        Cluster cluster = clusterRule.startCluster();
        createSomeData( clusterDatabase( cluster ) );
        return cluster;
    }

    public static DbRepresentation createSomeData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", "Neo" );
            node.setProperty( "random", Math.random() * 10000 );
            db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
        return DbRepresentation.of( db );
    }

    private IntFunction<String> addressWithIncrementingPortPerInstance( int initialPort )
    {
        return instanceId -> ip + ":" + (initialPort + instanceId);
    }

    private static CoreClusterMember clusterLeader( Cluster cluster )
    {
        return cluster.getDbWithRole( Role.LEADER );
    }

    private DbRepresentation getBackupDbRepresentation( String name )
    {
        return DbRepresentation.of( new File( backupDir, name ) );
    }

    private int runBackupToolFromOtherJvmToGetExitCode( String... args ) throws Exception
    {
        return TestHelpers.runBackupToolFromOtherJvmToGetExitCode( testDirectory.absolutePath(), args );
    }

}
