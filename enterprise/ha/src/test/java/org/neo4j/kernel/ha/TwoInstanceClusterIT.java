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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.kernel.impl.ha.ClusterManager.memberSeesOtherMemberAsFailed;

public class TwoInstanceClusterIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    private ClusterManager.ManagedCluster cluster;

    @Before
    public void setup()
    {
        cluster = clusterRule
                .withSharedSetting( HaSettings.read_timeout, "1s" )
                .withSharedSetting( HaSettings.state_switch_timeout, "2s" )
                .withSharedSetting( HaSettings.com_chunk_size, "1024" )
                .withCluster( clusterOfSize( 2 ) )
                .startCluster();
    }

    @Test
    public void masterShouldRemainAvailableIfTheSlaveDiesAndRecovers() throws Throwable
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase theSlave = cluster.getAnySlave();

        String propertyName = "prop";
        String propertyValue1 = "value1";
        String propertyValue2 = "value2";
        long masterNodeId;
        long slaveNodeId;

        ClusterManager.RepairKit repairKit = cluster.fail( theSlave );
        cluster.await( memberSeesOtherMemberAsFailed( master, theSlave ) );

        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode();
            node.setProperty( propertyName, propertyValue1 );
            masterNodeId = node.getId();
            tx.success();
        }

        repairKit.repair();

        cluster.await( allSeesAllAsAvailable() );

        try ( Transaction tx = theSlave.beginTx() )
        {
            Node node = theSlave.createNode();
            node.setProperty( propertyName, propertyValue2 );
            assertEquals( propertyValue1, theSlave.getNodeById( masterNodeId ).getProperty( propertyName ) );
            slaveNodeId = node.getId();
            tx.success();
        }

        try ( Transaction tx = master.beginTx() )
        {
            assertEquals( propertyValue2, master.getNodeById( slaveNodeId ).getProperty( propertyName ) );
            tx.success();
        }
    }

    @Test
    public void slaveShouldMoveToPendingAndThenRecoverIfMasterDiesAndThenRecovers() throws Throwable
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase theSlave = cluster.getAnySlave();

        String propertyName = "prop";
        String propertyValue = "value1";
        long slaveNodeId;

        ClusterManager.RepairKit repairKit = cluster.fail( master );
        cluster.await( memberSeesOtherMemberAsFailed( theSlave, master) );

        assertEquals( HighAvailabilityMemberState.PENDING, theSlave.getInstanceState() );

        repairKit.repair();

        cluster.await( allSeesAllAsAvailable() );

        try ( Transaction tx = theSlave.beginTx() )
        {
            Node node = theSlave.createNode();
            slaveNodeId = node.getId();
            node.setProperty( propertyName, propertyValue );
            tx.success();
        }

        try ( Transaction tx = master.beginTx() )
        {
            assertEquals( propertyValue, master.getNodeById( slaveNodeId ).getProperty( propertyName ) );
            tx.success();
        }
    }
}
