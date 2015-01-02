/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.kernel.configuration.Config.parseLongWithUnit;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;

import org.junit.Test;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.tooling.GlobalGraphOperations;

public class BiggerThanLogTxIT extends AbstractClusterTest
{
    private static final String ROTATION_THRESHOLD = "1M";

    @Test
    public void shouldHandleSlaveCommittingLargeTx() throws Exception
    {
        // GIVEN
        cluster.await( allSeesAllAsAvailable() );
        GraphDatabaseService slave = cluster.getAnySlave();
        int initialNodeCount = nodeCount( slave );;

        // WHEN
        int nodeCount = commitLargeTx( slave );
        cluster.sync();

        // THEN all should have that tx
        assertAllMembersHasNodeCount( initialNodeCount+nodeCount );
        // and if then master commits something, they should all get that too
        commitSmallTx( cluster.getMaster() );
        cluster.sync();
        assertAllMembersHasNodeCount( initialNodeCount+nodeCount+1 );
    }

    @Test
    public void shouldHandleMasterCommittingLargeTx() throws Exception
    {
        // GIVEN
        cluster.await( allSeesAllAsAvailable() );
        GraphDatabaseService slave = cluster.getAnySlave();
        int initialNodeCount = nodeCount( slave );

        // WHEN
        int nodeCount = commitLargeTx( cluster.getMaster() );
        cluster.sync();

        // THEN all should have that tx
        assertAllMembersHasNodeCount( initialNodeCount+nodeCount );
        // and if then master commits something, they should all get that too
        commitSmallTx( cluster.getMaster() );
        cluster.sync();
        assertAllMembersHasNodeCount( initialNodeCount+nodeCount+1 );
    }

    @Override
    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
    {
        builder.setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, ROTATION_THRESHOLD );
    }

    private void commitSmallTx( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private int nodeCount( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            int count = count( GlobalGraphOperations.at( db ).getAllNodes() );
            tx.success();
            return count;
        }
    }

    private void assertAllMembersHasNodeCount( int expectedNodeCount )
    {
        for ( GraphDatabaseService db : cluster.getAllMembers() )
        {
            assertEquals( expectedNodeCount, nodeCount( db ) );
        }
    }

    private int commitLargeTx( GraphDatabaseService db )
    {
        // We're not actually asserting that this transaction produces log data bigger than the threshold.
        long rotationThreshold = parseLongWithUnit( ROTATION_THRESHOLD );
        int nodeCount = 100;
        byte[] arrayProperty = new byte[(int) (rotationThreshold/nodeCount)];
        Transaction tx = db.beginTx();
        try
        {
            for ( int i = 0; i < nodeCount; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "name", "big" + i );
                node.setProperty( "data", arrayProperty );
            }
            tx.success();
            return nodeCount;
        }
        finally
        {
            tx.finish();
        }
    }
}
