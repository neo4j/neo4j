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
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReadReplicaStopStartIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( 2 ).withNumberOfReadReplicas( 1 );

    @Test
    public void shouldBeAbleToFreezeAndUnfreezeReadReplicaWhileClusterContinues() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode();
            node.setProperty( "name", "Jodie Whittaker" );
            tx.success();
        } );

        // when
        ReadReplicaGraphDatabase readReplica = cluster.findAnyReadReplica().database();

        // then
        Object firstStatus = readReplica.execute( "CALL dbms.cluster.freezeReadReplica()" ).next().get( "status" );
        assertEquals( String.class, firstStatus.getClass() );
        assertEquals( "frozen",firstStatus );

        // when
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode();
            node.setProperty( "name", "Peter Capaldi" );
            tx.success();
        } );

        // then
        Object secondStatus = readReplica.execute( "CALL dbms.cluster.unfreezeReadReplica()" ).next().get( "status" );
        assertEquals( String.class, secondStatus.getClass() );
        assertEquals( "unfrozen", secondStatus );

        try ( Transaction tx = readReplica.beginTx() )
        {
            ThrowingSupplier<Long,Exception> nodeCount = () -> count( readReplica.getAllNodes() );
            assertEventually( "Read replica should have restarted and caught up", nodeCount, is( 2L ), 1, MINUTES );
            tx.success();
        }
    }
}
