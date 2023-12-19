/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.scenarios;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assume.assumeTrue;

public class ClusterIdReuseIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            // increased to decrease likelihood of unnecessary leadership changes
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "2s" )
            // need to be 1 in order for the reuse count to be deterministic
            .withSharedCoreParam( GraphDatabaseSettings.record_id_batch_size, Integer.toString( 1 ) )
            .withNumberOfReadReplicas( 0 );
    private Cluster cluster;

    @Test
    public void shouldReuseIdsInCluster() throws Exception
    {
        cluster = clusterRule.startCluster();

        final MutableLong first = new MutableLong();
        final MutableLong second = new MutableLong();

        CoreClusterMember leader1 = createThreeNodes( cluster, first, second );
        CoreClusterMember leader2 = removeTwoNodes( cluster, first, second );

        assumeTrue( leader1 != null && leader1.equals( leader2 ) );

        // Force maintenance on leader
        idMaintenanceOnLeader( leader1 );
        IdGeneratorFactory idGeneratorFactory = resolveDependency( leader1, IdGeneratorFactory.class );
        final IdGenerator idGenerator = idGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, idGenerator.getDefragCount() );

        final MutableLong node1id = new MutableLong();
        final MutableLong node2id = new MutableLong();
        final MutableLong node3id = new MutableLong();

        CoreClusterMember clusterMember = cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Node node3 = db.createNode();

            node1id.setValue( node1.getId() );
            node2id.setValue( node2.getId() );
            node3id.setValue( node3.getId() );

            tx.success();
        } );

        assumeTrue( leader1.equals( clusterMember ) );

        assertEquals( first.longValue(), node1id.longValue() );
        assertEquals( second.longValue(), node2id.longValue() );
        assertEquals( idGenerator.getHighestPossibleIdInUse(), node3id.longValue() );
    }

    @Test
    public void newLeaderShouldNotReuseIds() throws Exception
    {
        cluster = clusterRule.startCluster();

        final MutableLong first = new MutableLong();
        final MutableLong second = new MutableLong();

        CoreClusterMember creationLeader = createThreeNodes( cluster, first, second );
        CoreClusterMember deletionLeader = removeTwoNodes( cluster, first, second );

        // the following assumption is not sufficient for the subsequent assertions, since leadership is a volatile state
        assumeTrue( creationLeader != null && creationLeader.equals( deletionLeader ) );

        idMaintenanceOnLeader( creationLeader );
        IdGeneratorFactory idGeneratorFactory = resolveDependency( creationLeader, IdGeneratorFactory.class );
        IdGenerator creationLeaderIdGenerator = idGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, creationLeaderIdGenerator.getDefragCount() );

        // Force leader switch
        cluster.removeCoreMemberWithServerId( creationLeader.serverId() );

        // waiting for new leader
        CoreClusterMember newLeader = cluster.awaitLeader();
        assertNotSame( creationLeader.serverId(), newLeader.serverId() );
        idMaintenanceOnLeader( newLeader );

        IdGeneratorFactory newLeaderIdGeneratorFactory = resolveDependency( newLeader, IdGeneratorFactory.class );
        final IdGenerator idGenerator = newLeaderIdGeneratorFactory.get( IdType.NODE );
        assertEquals( 0, idGenerator.getDefragCount() );

        CoreClusterMember newCreationLeader = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode();
            assertEquals( idGenerator.getHighestPossibleIdInUse(), node.getId() );

            tx.success();
        } );
        assumeTrue( newLeader.equals( newCreationLeader ) );
    }

    @Test
    public void reusePreviouslyFreedIds() throws Exception
    {
        cluster = clusterRule.startCluster();

        final MutableLong first = new MutableLong();
        final MutableLong second = new MutableLong();

        CoreClusterMember creationLeader = createThreeNodes( cluster, first, second );
        CoreClusterMember deletionLeader = removeTwoNodes( cluster, first, second );

        assumeTrue( creationLeader != null && creationLeader.equals( deletionLeader ) );
        IdGeneratorFactory idGeneratorFactory = resolveDependency( creationLeader, IdGeneratorFactory.class );
        idMaintenanceOnLeader( creationLeader );
        IdGenerator creationLeaderIdGenerator = idGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, creationLeaderIdGenerator.getDefragCount() );

        // Restart and re-elect first leader
        cluster.removeCoreMemberWithServerId( creationLeader.serverId() );
        cluster.addCoreMemberWithId( creationLeader.serverId() ).start();

        CoreClusterMember leader = cluster.awaitLeader();
        while ( leader.serverId() != creationLeader.serverId() )
        {
            cluster.removeCoreMemberWithServerId( leader.serverId() );
            cluster.addCoreMemberWithId( leader.serverId() ).start();
            leader = cluster.awaitLeader();
        }

        idMaintenanceOnLeader( leader );
        IdGeneratorFactory leaderIdGeneratorFactory = resolveDependency( leader, IdGeneratorFactory.class );
        creationLeaderIdGenerator = leaderIdGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, creationLeaderIdGenerator.getDefragCount() );

        final MutableLong node1id = new MutableLong();
        final MutableLong node2id = new MutableLong();
        CoreClusterMember reuseLeader = cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();

            node1id.setValue( node1.getId() );
            node2id.setValue( node2.getId() );

            tx.success();
        } );
        assumeTrue( leader.equals( reuseLeader ) );

        assertEquals( first.longValue(), node1id.longValue() );
        assertEquals( second.longValue(), node2id.longValue() );
    }

    private void idMaintenanceOnLeader( CoreClusterMember leader )
    {
        IdController idController = resolveDependency( leader, IdController.class );
        idController.maintenance();
    }

    private <T> T resolveDependency( CoreClusterMember leader, Class<T> clazz )
    {
        return leader.database().getDependencyResolver().resolveDependency( clazz );
    }

    private CoreClusterMember removeTwoNodes( Cluster cluster, MutableLong first, MutableLong second ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.getNodeById( first.longValue() );
            node1.delete();

            db.getNodeById( second.longValue() ).delete();

            tx.success();
        } );
    }

    private CoreClusterMember createThreeNodes( Cluster cluster, MutableLong first, MutableLong second ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
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
