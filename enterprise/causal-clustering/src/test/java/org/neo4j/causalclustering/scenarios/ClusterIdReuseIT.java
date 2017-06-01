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
package org.neo4j.causalclustering.scenarios;


import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class ClusterIdReuseIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );
    private Cluster cluster;

    @Before
    public void setUp() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldReuseIdsInCluster() throws Exception
    {
        final MutableLong first = new MutableLong();
        final MutableLong second = new MutableLong();

        createThreeNodes( cluster, first, second );
        removeTwoNodes( cluster, first, second );

        // Force maintenance on leader
        IdController idController = idMaintenanceOnLeader();

        final IdGenerator idGenerator = idController.getIdGeneratorFactory().get( IdType.NODE );
        assertEquals( 2, idGenerator.getDefragCount() );

        cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Node node3 = db.createNode();

            assertEquals( first.longValue(), node1.getId() );
            assertEquals( second.longValue(), node2.getId() );
            assertEquals( idGenerator.getHighestPossibleIdInUse(), node3.getId() );

            tx.success();
        } );
    }

    @Test
    public void newLeaderShouldNotReuseIds() throws Exception
    {
        final MutableLong first = new MutableLong();
        final MutableLong second = new MutableLong();

        createThreeNodes( cluster, first, second );
        removeTwoNodes( cluster, first, second );

        IdGenerator oldLeaderIdGenerator = idMaintenanceOnLeader().getIdGeneratorFactory().get( IdType.NODE );
        assertEquals( 2, oldLeaderIdGenerator.getDefragCount() );

        // Force leader switch
        CoreClusterMember firstLeader = cluster.awaitLeader();
        cluster.removeCoreMemberWithMemberId( firstLeader.serverId() );

        // waiting for new leader
        CoreClusterMember newLeader = cluster.awaitLeader();
        assertNotSame( firstLeader.serverId(), newLeader.serverId() );
        IdController idController = idMaintenanceOnLeader();

        final IdGenerator idGenerator = idController.getIdGeneratorFactory().get( IdType.NODE );
        assertEquals( 0, idGenerator.getDefragCount() );

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode();
            assertEquals( idGenerator.getHighestPossibleIdInUse(), node.getId() );

            tx.success();
        } );

        // Re-elect first leader
        cluster.addCoreMemberWithId( firstLeader.serverId() ).start();

        CoreClusterMember leader = cluster.awaitLeader();
        while ( leader.serverId() != firstLeader.serverId() )
        {
            cluster.removeCoreMemberWithMemberId( leader.serverId() );
            cluster.addCoreMemberWithId( leader.serverId() ).start();
            leader = cluster.awaitLeader();
        }

        oldLeaderIdGenerator = idMaintenanceOnLeader().getIdGeneratorFactory().get( IdType.NODE );
        assertEquals( 2, oldLeaderIdGenerator.getDefragCount() );

        cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();

            assertEquals( first.longValue(), node1.getId() );
            assertEquals( second.longValue(), node2.getId() );

            tx.success();
        } );
    }

    private IdController idMaintenanceOnLeader() throws TimeoutException
    {
        CoreClusterMember newLeader = cluster.awaitLeader();
        IdController idController = newLeader.database().getDependencyResolver().resolveDependency( IdController.class );
        idController.maintenance();
        return idController;
    }

    private void removeTwoNodes( Cluster cluster, MutableLong first, MutableLong second ) throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.getNodeById( first.longValue() );
            node1.delete();

            db.getNodeById( second.longValue() ).delete();

            tx.success();
        } );
    }

    private void createThreeNodes( Cluster cluster, MutableLong first, MutableLong second ) throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.createNode();
            first.setValue( node1.getId() );

            Node node2 = db.createNode();
            second.setValue( node2.getId() );

            db.createNode();

            tx.success();
        } );
    }
}
