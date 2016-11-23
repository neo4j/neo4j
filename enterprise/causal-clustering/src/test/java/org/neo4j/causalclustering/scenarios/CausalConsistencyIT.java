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
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.time.Duration.ofSeconds;
import static org.junit.Assert.fail;

public class CausalConsistencyIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( CausalConsistencyIT.class )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 1 );

    @Test
    public void transactionsCommittedInTheCoreShouldAppearOnTheReadReplica() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        cluster.coreTx( ( coreGraphDatabase, transaction ) -> {
            coreGraphDatabase.createNode();
            transaction.success();
        } );

        CoreGraphDatabase leaderDatabase = cluster.awaitLeader().database();
        long transactionVisibleOnLeader = transactionIdTracker( leaderDatabase ).newestEncounteredTxId();

        // then
        ReadReplicaGraphDatabase readReplicaGraphDatabase = cluster.findAnyReadReplica().database();
        transactionIdTracker( readReplicaGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 3 ) );
    }

    @Test
    public void transactionsShouldNotAppearOnTheReadReplicaWhilePollingIsPaused() throws Throwable
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        ReadReplicaGraphDatabase readReplicaGraphDatabase = cluster.findAnyReadReplica().database();
        CatchupPollingProcess pollingClient = readReplicaGraphDatabase.getDependencyResolver()
                .resolveDependency( CatchupPollingProcess.class );
        pollingClient.stop();

        cluster.coreTx( ( coreGraphDatabase, transaction ) -> {
            coreGraphDatabase.createNode();
            transaction.success();
        } );

        CoreGraphDatabase leaderDatabase = cluster.awaitLeader().database();
        long transactionVisibleOnLeader = transactionIdTracker( leaderDatabase ).newestEncounteredTxId();

        // when the poller is paused, transaction doesn't make it to the read replica
        try
        {
            transactionIdTracker( readReplicaGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 3 ) );
            fail( "should have thrown exception" );
        }
        catch ( TransactionFailureException e )
        {
            // expected timeout
        }

        // when the poller is resumed, it does make it to the read replica
        pollingClient.start();
        transactionIdTracker( readReplicaGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 3 ) );
    }

    private TransactionIdTracker transactionIdTracker( GraphDatabaseAPI database )
    {
        TransactionIdStore transactionIdStore =
                database.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        AvailabilityGuard availabilityGuard =
                database.getDependencyResolver().resolveDependency( AvailabilityGuard.class );
        return new TransactionIdTracker( transactionIdStore, availabilityGuard );
    }
}
