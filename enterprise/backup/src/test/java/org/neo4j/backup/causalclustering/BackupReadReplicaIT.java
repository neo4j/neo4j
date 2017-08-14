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
package org.neo4j.backup.causalclustering;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.backup.causalclustering.BackupCoreIT.backupArguments;
import static org.neo4j.backup.causalclustering.BackupCoreIT.createSomeData;
import static org.neo4j.backup.causalclustering.BackupCoreIT.getConfig;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.util.JvmRunner.runBackupToolFromOtherJvmToGetExitCode;

public class BackupReadReplicaIT
{
    private int backupPort = findFreePort( 22000, 23000);

    @Rule
    public ClusterRule clusterRule = new ClusterRule( BackupReadReplicaIT.class ).withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withNumberOfReadReplicas( 1 )
            .withSharedReadReplicaParam( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
            .withInstanceReadReplicaParam( OnlineBackupSettings.online_backup_server, serverId -> ":" + backupPort );

    private Cluster cluster;
    private File backupPath;

    @Before
    public void setup() throws Exception
    {
        backupPath = clusterRule.testDirectory().cleanDirectory( "backup-db" );
        cluster = clusterRule.startCluster();
    }

    private boolean readReplicasUpToDateAsTheLeader( CoreGraphDatabase leader, ReadReplicaGraphDatabase readReplica )
    {
        long leaderTxId = leader.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastClosedTransactionId();
        long lastClosedTxId = readReplica.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastClosedTransactionId();
        return lastClosedTxId == leaderTxId;
    }

    @Test
    public void makeSureBackupCanBePerformed() throws Throwable
    {
        // Run backup
        CoreGraphDatabase leader = createSomeData( cluster );

        ReadReplicaGraphDatabase readReplica = cluster.findAnyReadReplica().database();

        awaitEx( () -> readReplicasUpToDateAsTheLeader( leader, readReplica ), 1, TimeUnit.MINUTES );

        DbRepresentation beforeChange = DbRepresentation.of( readReplica );
        String backupAddress = this.backupAddress( readReplica );

        String[] args = backupArguments( backupAddress, backupPath, "readreplica" );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( clusterRule.clusterDirectory(), args ) );

        // Add some new data
        DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

        // Verify that backed up database can be started and compare representation
        DbRepresentation backupRepresentation =
                DbRepresentation.of( new File( backupPath, "readreplica" ), getConfig() );
        assertEquals( beforeChange, backupRepresentation );
        assertNotEquals( backupRepresentation, afterChange );
    }

    private String backupAddress( ReadReplicaGraphDatabase readReplica )
    {
        InetSocketAddress inetSocketAddress = readReplica.getDependencyResolver()
                .resolveDependency( Config.class ).get( CausalClusteringSettings.transaction_advertised_address )
                .socketAddress();
        return inetSocketAddress.getHostName() + ":" + backupPort;
    }

    private static int findFreePort( int startRange, int endRange )
    {
        InetSocketAddress address = null;
        RuntimeException ex = null;
        for ( int port = startRange; port <= endRange; port++ )
        {
            address = new InetSocketAddress( port );

            try
            {
                new ServerSocket( address.getPort(), 100, address.getAddress() ).close();
                ex = null;
                break;
            }
            catch ( IOException e )
            {
                ex = new RuntimeException( e );
            }
        }
        if ( ex != null )
        {
            throw ex;
        }
        assert address != null;
        return address.getPort();
    }
}
