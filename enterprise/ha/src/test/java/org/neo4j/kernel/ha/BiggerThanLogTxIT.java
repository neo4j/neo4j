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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.TransactionTemplate;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.kernel.configuration.Config.parseLongWithUnit;

public class BiggerThanLogTxIT
{
    private static final String ROTATION_THRESHOLD = "1M";

    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( GraphDatabaseSettings.logical_log_rotation_threshold, ROTATION_THRESHOLD );

    protected ClusterManager.ManagedCluster cluster;

    private TransactionTemplate template = new TransactionTemplate().retries( 10 ).backoff( 3, TimeUnit.SECONDS );

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldHandleSlaveCommittingLargeTx() throws Exception
    {
        // GIVEN
        GraphDatabaseService slave = cluster.getAnySlave();
        int initialNodeCount = nodeCount( slave );

        // WHEN
        cluster.info( "Before commit large" );
        int nodeCount = commitLargeTx( slave );
        cluster.info( "Before sync" );
        cluster.sync();
        cluster.info( "After sync" );

        // THEN all should have that tx
        assertAllMembersHasNodeCount( initialNodeCount + nodeCount );
        // and if then master commits something, they should all get that too
        cluster.info( "Before commit small" );
        commitSmallTx( cluster.getMaster() );
        cluster.info( "Before sync small" );
        cluster.sync();
        cluster.info( "After sync small" );
        assertAllMembersHasNodeCount( initialNodeCount + nodeCount + 1 );
    }

    @Test
    public void shouldHandleMasterCommittingLargeTx() throws Exception
    {
        // GIVEN
        GraphDatabaseService slave = cluster.getAnySlave();
        int initialNodeCount = nodeCount( slave );

        // WHEN
        int nodeCount = commitLargeTx( cluster.getMaster() );
        cluster.sync();

        // THEN all should have that tx
        assertAllMembersHasNodeCount( initialNodeCount + nodeCount );
        // and if then master commits something, they should all get that too
        commitSmallTx( cluster.getMaster() );
        cluster.sync();
        assertAllMembersHasNodeCount( initialNodeCount + nodeCount + 1 );
    }

    private void commitSmallTx( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            db.createNode();
            transaction.success();
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
            // Try again with sync, it will clear up...
            if ( expectedNodeCount != nodeCount( db ) )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    try
                    {
                        Thread.sleep(1000);
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }

                    int count = nodeCount( db );
                    if (expectedNodeCount == count)
                        break;

                    try
                    {
                        cluster.sync(  );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }

            assertEquals( expectedNodeCount, nodeCount( db ) );
        }
    }

    private int commitLargeTx( final GraphDatabaseService db )
    {
        return template.with( db ).execute( new Function<Transaction,Integer>()
        {
            @Override
            public Integer apply( Transaction transaction ) throws RuntimeException
            {
                // We're not actually asserting that this transaction produces log data
                // bigger than the threshold.
                long rotationThreshold = parseLongWithUnit( ROTATION_THRESHOLD );
                int nodeCount = 100;
                byte[] arrayProperty = new byte[(int) (rotationThreshold / nodeCount)];
                for ( int i = 0; i < nodeCount; i++ )
                {
                    Node node = db.createNode();
                    node.setProperty( "name", "big" + i );
                    node.setProperty( "data", arrayProperty );
                }
                return nodeCount;
            }
        } );
    }
}
