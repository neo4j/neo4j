/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.helpers.collection.Iterables.count;

/**
 * This isn't a deterministic test, but instead tries to trigger a race condition
 * for a couple of seconds. The original issues is mostly seen immediately, but after
 * a fix is in this test will take the full amount of seconds unfortunately.
 */
public class TestConcurrentRelationshipChainLoadingIssue
{
    private final int relCount = 2;

    @Test
    public void tryToTriggerRelationshipLoadingStoppingMidWay() throws Throwable
    {
        tryToTriggerRelationshipLoadingStoppingMidWay( 50 );
    }

    @Test
    public void tryToTriggerRelationshipLoadingStoppingMidWayForDenseNodeRepresentation() throws Throwable
    {
        tryToTriggerRelationshipLoadingStoppingMidWay( 1 );
    }

    private void tryToTriggerRelationshipLoadingStoppingMidWay( int denseNodeThreshold ) throws Throwable
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( dense_node_threshold, "" + denseNodeThreshold )
                .newGraphDatabase();
        Node node = createNodeWithRelationships( db );

        checkStateToHelpDiagnoseFlakeyTest( db, node );

        long end = currentTimeMillis() + SECONDS.toMillis( 5 );
        int iterations = 0;
        while ( currentTimeMillis() < end && iterations < 100 )
        {
            tryOnce( db, node );
            iterations++;
        }

        db.shutdown();
    }

    private void checkStateToHelpDiagnoseFlakeyTest( GraphDatabaseAPI db, Node node )
    {
        loadNode( db, node );
        loadNode( db, node );
    }

    private void loadNode( GraphDatabaseAPI db, Node node )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            Iterables.count( node.getRelationships() );
        }
    }

    private void tryOnce( final GraphDatabaseAPI db, final Node node ) throws Throwable
    {
        Race race = new Race().withRandomStartDelays();
        race.addContestants( Runtime.getRuntime().availableProcessors(), () ->
        {
            try ( Transaction ignored = db.beginTx() )
            {
                assertEquals( relCount, count( node.getRelationships() ) );
            }
        } );
        race.go();
    }

    private Node createNodeWithRelationships( GraphDatabaseAPI db )
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < relCount / 2; i++ )
            {
                node.createRelationshipTo( node, MyRelTypes.TEST );
            }
            for ( int i = 0; i < relCount / 2; i++ )
            {
                node.createRelationshipTo( node, MyRelTypes.TEST2 );
            }
            tx.success();
            return node;
        }
    }
}
