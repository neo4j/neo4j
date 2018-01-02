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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class HACountsPropagationTest
{
    private static final int PULL_INTERVAL = 100;

    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( HaSettings.pull_interval, PULL_INTERVAL + "ms" );

    @Test
    public void shouldPropagateNodeCountsInHA() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            master.createNode();
            master.createNode( DynamicLabel.label( "A" ) );
            tx.success();
        }

        waitForPullUpdates();

        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            CountsTracker counts = db.getDependencyResolver().resolveDependency( NeoStores.class ).getCounts();
            assertEquals( 2, counts.nodeCount( -1, newDoubleLongRegister() ).readSecond() );
            assertEquals( 1, counts.nodeCount( 0 /* A */, newDoubleLongRegister() ).readSecond() );
        }
    }

    @Test
    public void shouldPropagateRelationshipCountsInHA() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            Node left = master.createNode();
            Node right = master.createNode( DynamicLabel.label( "A" ) );
            left.createRelationshipTo( right, DynamicRelationshipType.withName( "Type" ) );
            tx.success();
        }

        waitForPullUpdates();

        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            CountsTracker counts = db.getDependencyResolver().resolveDependency( NeoStores.class ).getCounts();
            assertEquals( 1, counts.relationshipCount( -1, -1, -1, newDoubleLongRegister() ).readSecond() );
            assertEquals( 1, counts.relationshipCount( -1, -1, 0, newDoubleLongRegister() ).readSecond() );
            assertEquals( 1, counts.relationshipCount( -1, 0, -1, newDoubleLongRegister() ).readSecond() );
            assertEquals( 1, counts.relationshipCount( -1, 0, 0, newDoubleLongRegister() ).readSecond() );
        }
    }

    private void waitForPullUpdates()
    {
        try
        {
            Thread.sleep( PULL_INTERVAL * 2 );
        }
        catch ( InterruptedException ex )
        {
            // ignore me
        }
    }
}
