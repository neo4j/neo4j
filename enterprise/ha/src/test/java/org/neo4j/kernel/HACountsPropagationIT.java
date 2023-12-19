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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class HACountsPropagationIT
{
    private static final int PULL_INTERVAL = 100;

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( HaSettings.pull_interval, PULL_INTERVAL + "ms" );

    @Test
    public void shouldPropagateNodeCountsInHA()
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            master.createNode();
            master.createNode( Label.label( "A" ) );
            tx.success();
        }

        cluster.sync();

        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            CountsTracker counts = counts( db );
            assertEquals( 2, counts.nodeCount( -1, newDoubleLongRegister() ).readSecond() );
            assertEquals( 1, counts.nodeCount( 0 /* A */, newDoubleLongRegister() ).readSecond() );
        }
    }

    private CountsTracker counts( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getCounts();
    }

    @Test
    public void shouldPropagateRelationshipCountsInHA()
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            Node left = master.createNode();
            Node right = master.createNode( Label.label( "A" ) );
            left.createRelationshipTo( right, RelationshipType.withName( "Type" ) );
            tx.success();
        }

        cluster.sync();

        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            CountsTracker counts = counts( db );
            assertEquals( 1, counts.relationshipCount( -1, -1, -1, newDoubleLongRegister() ).readSecond() );
            assertEquals( 1, counts.relationshipCount( -1, -1, 0, newDoubleLongRegister() ).readSecond() );
            assertEquals( 1, counts.relationshipCount( -1, 0, -1, newDoubleLongRegister() ).readSecond() );
            assertEquals( 1, counts.relationshipCount( -1, 0, 0, newDoubleLongRegister() ).readSecond() );
        }
    }
}
