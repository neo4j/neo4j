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
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    private ClusterManager.ManagedCluster cluster;

    @Before
    public void setup() throws Exception
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
