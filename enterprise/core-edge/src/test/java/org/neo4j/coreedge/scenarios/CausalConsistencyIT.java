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
package org.neo4j.coreedge.scenarios;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.catchup.tx.TxPollingClient;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.edge.EdgeGraphDatabase;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.coreedge.ClusterRule;

import static java.time.Duration.ofSeconds;

import static org.junit.Assert.fail;

public class CausalConsistencyIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( CausalConsistencyIT.class )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 1 );

    @Test
    public void transactionsCommittedInTheCoreShouldAppearOnTheEdge() throws Exception
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
        EdgeGraphDatabase edgeGraphDatabase = cluster.findAnEdgeMember().database();
        transactionIdTracker( edgeGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 3 ) );
    }

    @Test
    public void transactionsShouldNotAppearOnTheEdgeWhilePollingIsPaused() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        EdgeGraphDatabase edgeGraphDatabase = cluster.findAnEdgeMember().database();
        TxPollingClient pollingClient = edgeGraphDatabase.getDependencyResolver()
                .resolveDependency( TxPollingClient.class );
        pollingClient.pause();

        cluster.coreTx( ( coreGraphDatabase, transaction ) -> {
            coreGraphDatabase.createNode();
            transaction.success();
        } );

        CoreGraphDatabase leaderDatabase = cluster.awaitLeader().database();
        long transactionVisibleOnLeader = transactionIdTracker( leaderDatabase ).newestEncounteredTxId();

        // when the poller is paused, transaction doesn't make it to the edge server
        try
        {
            transactionIdTracker( edgeGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 3 ) );
            fail( "should have thrown exception" );
        }
        catch ( TransactionFailureException e )
        {
            // expected timeout
        }

        // when the poller is resumed, it does make it to the edge server
        pollingClient.resume();
        transactionIdTracker( edgeGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 3 ) );
    }

    private TransactionIdTracker transactionIdTracker( GraphDatabaseAPI database )
    {
        return new TransactionIdTracker( database
                    .getDependencyResolver().resolveDependency( TransactionIdStore.class ) );
    }
}
