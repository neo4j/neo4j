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
package org.neo4j.causalclustering;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.causalclustering.BackupCoreIT.backupArguments;
import static org.neo4j.causalclustering.BackupCoreIT.createSomeData;
import static org.neo4j.causalclustering.BackupCoreIT.getConfig;
import static org.neo4j.causalclustering.helpers.CausalClusteringTestHelpers.transactionAddress;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;

public class BackupReadReplicaIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withNumberOfReadReplicas( 1 )
            .withSharedReadReplicaParam( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );

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
        String backupAddress = transactionAddress( readReplica );

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
}
