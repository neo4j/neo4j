/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.TransactionTemplate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.configuration.Settings.parseLongWithUnit;

public class BiggerThanLogTxIT
{
    private static final String ROTATION_THRESHOLD = "1M";

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( GraphDatabaseSettings.logical_log_rotation_threshold, ROTATION_THRESHOLD );

    protected ClusterManager.ManagedCluster cluster;

    private final TransactionTemplate template = new TransactionTemplate().retries( 10 ).backoff( 3, TimeUnit.SECONDS );

    @Before
    public void setup()
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldHandleSlaveCommittingLargeTx()
    {
        // GIVEN
        GraphDatabaseService slave = cluster.getAnySlave();
        long initialNodeCount = nodeCount( slave );

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
    public void shouldHandleMasterCommittingLargeTx()
    {
        // GIVEN
        GraphDatabaseService slave = cluster.getAnySlave();
        long initialNodeCount = nodeCount( slave );

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

    private long nodeCount( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long count = Iterables.count( db.getAllNodes() );
            tx.success();
            return count;
        }
    }

    private void assertAllMembersHasNodeCount( long expectedNodeCount )
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

                    long count = nodeCount( db );
                    if ( expectedNodeCount == count )
                    {
                        break;
                    }

                    cluster.sync(  );
                }
            }

            assertEquals( expectedNodeCount, nodeCount( db ) );
        }
    }

    private int commitLargeTx( final GraphDatabaseService db )
    {
        return template.with( db ).execute( transaction ->
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
        } );
    }
}
