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
package org.neo4j.coreedge.backup;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.edge.EdgeGraphDatabase;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.coreedge.backup.BackupCoreIT.backupAddress;
import static org.neo4j.coreedge.backup.BackupCoreIT.backupArguments;
import static org.neo4j.coreedge.backup.BackupCoreIT.createSomeData;
import static org.neo4j.coreedge.backup.BackupCoreIT.getConfig;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.test.rule.SuppressOutput.suppress;

public class BackupEdgeIT
{
    @Rule
    public SuppressOutput suppressOutput = suppress( SuppressOutput.System.out, SuppressOutput.System.err );

    @Rule
    public ClusterRule clusterRule = new ClusterRule( BackupEdgeIT.class )
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withNumberOfEdgeMembers( 1 )
            .withSharedEdgeParam( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
            .withInstanceEdgeParam( OnlineBackupSettings.online_backup_server, serverId -> ":" + findFreePort( 8000, 9000 ) );

    private Cluster cluster;
    private File backupPath;

    @Before
    public void setup() throws Exception
    {
        backupPath = clusterRule.testDirectory().cleanDirectory( "backup-db" );
        cluster = clusterRule.startCluster();
    }

    private boolean edgesUpToDateAsTheLeader( CoreGraphDatabase leader, EdgeGraphDatabase edgeClusterMember )
    {
        long leaderTxId = leader.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
        long lastClosedTxId = edgeClusterMember.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
        return lastClosedTxId == leaderTxId;
    }

    @Test
    public void makeSureBackupCanBePerformed() throws Throwable
    {
        // Run backup
        CoreGraphDatabase leader = createSomeData( cluster );

        EdgeGraphDatabase edgeServer = cluster.findAnEdgeMember().database();

        awaitEx( () -> edgesUpToDateAsTheLeader( leader, edgeServer ), 1, TimeUnit.MINUTES );

        DbRepresentation beforeChange = DbRepresentation.of( edgeServer );
        String backupAddress = backupAddress( edgeServer );
        System.out.println( backupAddress );
        String[] args = backupArguments( backupAddress, backupPath.getPath() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( args ) );

        // Add some new data
        DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

        // Verify that backed up database can be started and compare representation
        DbRepresentation backupRepresentation = DbRepresentation.of( backupPath, getConfig() );
        assertEquals( beforeChange, backupRepresentation );
        assertNotEquals( backupRepresentation, afterChange );
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
