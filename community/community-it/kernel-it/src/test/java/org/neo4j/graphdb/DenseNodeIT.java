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
package org.neo4j.graphdb;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.Iterators.asResourceIterator;

public class DenseNodeIT
{
    @Rule
    public ImpermanentDatabaseRule databaseRule = new ImpermanentDatabaseRule();

    @Test
    public void testBringingNodeOverDenseThresholdIsConsistent()
    {
        // GIVEN
        GraphDatabaseService db = databaseRule.getGraphDatabaseAPI();

        Node root;
        try ( Transaction tx = db.beginTx() )
        {
            root = db.createNode();
            createRelationshipsOnNode( db, root, 40 );
            tx.success();
        }

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            createRelationshipsOnNode( db, root, 60 );

            // THEN
            assertEquals( 100, root.getDegree() );
            assertEquals( 100, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );
            assertEquals( 25, root.getDegree( RelationshipType.withName( "Type0" ) ) );
            assertEquals( 25, root.getDegree( RelationshipType.withName( "Type1" ) ) );
            assertEquals( 25, root.getDegree( RelationshipType.withName( "Type2" ) ) );
            assertEquals( 25, root.getDegree( RelationshipType.withName( "Type3" ) ) );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 100, root.getDegree() );
            assertEquals( 100, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );
            assertEquals( 25, root.getDegree( RelationshipType.withName( "Type0" ) ) );
            assertEquals( 25, root.getDegree( RelationshipType.withName( "Type1" ) ) );
            assertEquals( 25, root.getDegree( RelationshipType.withName( "Type2" ) ) );
            assertEquals( 25, root.getDegree( RelationshipType.withName( "Type3" ) ) );
            tx.success();
        }
    }

    @Test
    public void deletingRelationshipsFromDenseNodeIsConsistent()
    {
        // GIVEN
        GraphDatabaseService db = databaseRule.getGraphDatabaseAPI();

        Node root;
        try ( Transaction tx = db.beginTx() )
        {
            root = db.createNode();
            createRelationshipsOnNode( db, root, 100 );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            deleteRelationshipsFromNode( root, 80 );

            assertEquals( 20, root.getDegree() );
            assertEquals( 20, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 20, root.getDegree() );
            assertEquals( 20, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );
            tx.success();
        }
    }

    @Test
    public void movingBilaterallyOfTheDenseNodeThresholdIsConsistent()
    {
        // GIVEN
        GraphDatabaseService db = databaseRule.getGraphDatabaseAPI();

        Node root;
        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            root = db.createNode();
            createRelationshipsOnNode( db, root, 100 );
            deleteRelationshipsFromNode( root, 80 );

            assertEquals( 20, root.getDegree() );
            assertEquals( 20, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );

            tx.success();
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 20, root.getDegree() );
            assertEquals( 20, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );
            tx.success();
        }
    }

    @Test
    public void testBringingTwoConnectedNodesOverDenseThresholdIsConsistent()
    {
        // GIVEN
        GraphDatabaseService db = databaseRule.getGraphDatabaseAPI();

        Node source;
        Node sink;
        try ( Transaction tx = db.beginTx() )
        {
            source = db.createNode();
            sink = db.createNode();
            createRelationshipsBetweenNodes( source, sink, 40 );
            tx.success();
        }

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            createRelationshipsBetweenNodes( source, sink, 60 );

            // THEN
            assertEquals( 100, source.getDegree() );
            assertEquals( 100, source.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, source.getDegree( Direction.INCOMING ) );
            assertEquals( 25, source.getDegree( RelationshipType.withName( "Type0" ) ) );
            assertEquals( 25, source.getDegree( RelationshipType.withName( "Type1" ) ) );
            assertEquals( 25, source.getDegree( RelationshipType.withName( "Type2" ) ) );
            assertEquals( 25, source.getDegree( RelationshipType.withName( "Type3" ) ) );

            assertEquals( 100, sink.getDegree() );
            assertEquals( 0, sink.getDegree( Direction.OUTGOING ) );
            assertEquals( 100, sink.getDegree( Direction.INCOMING ) );
            assertEquals( 25, sink.getDegree( RelationshipType.withName( "Type0" ) ) );
            assertEquals( 25, sink.getDegree( RelationshipType.withName( "Type1" ) ) );
            assertEquals( 25, sink.getDegree( RelationshipType.withName( "Type2" ) ) );
            assertEquals( 25, sink.getDegree( RelationshipType.withName( "Type3" ) ) );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 100, source.getDegree() );
            assertEquals( 100, source.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, source.getDegree( Direction.INCOMING ) );
            assertEquals( 25, source.getDegree( RelationshipType.withName( "Type0" ) ) );
            assertEquals( 25, source.getDegree( RelationshipType.withName( "Type1" ) ) );
            assertEquals( 25, source.getDegree( RelationshipType.withName( "Type2" ) ) );
            assertEquals( 25, source.getDegree( RelationshipType.withName( "Type3" ) ) );

            assertEquals( 100, sink.getDegree() );
            assertEquals( 0, sink.getDegree( Direction.OUTGOING ) );
            assertEquals( 100, sink.getDegree( Direction.INCOMING ) );
            assertEquals( 25, sink.getDegree( RelationshipType.withName( "Type0" ) ) );
            assertEquals( 25, sink.getDegree( RelationshipType.withName( "Type1" ) ) );
            assertEquals( 25, sink.getDegree( RelationshipType.withName( "Type2" ) ) );
            assertEquals( 25, sink.getDegree( RelationshipType.withName( "Type3" ) ) );
            tx.success();
        }
    }

    @Test
    public void shouldBeAbleToCreateRelationshipsInEmptyDenseNode()
    {
        // GIVEN
        Node node;
        try ( Transaction tx = databaseRule.beginTx() )
        {
            node = databaseRule.createNode();
            createRelationshipsBetweenNodes( node, databaseRule.createNode(), denseNodeThreshold( databaseRule ) + 1 );
            tx.success();
        }
        try ( Transaction tx = databaseRule.beginTx() )
        {
            node.getRelationships().forEach( Relationship::delete );
            tx.success();
        }

        // WHEN
        Relationship rel;
        try ( Transaction tx = databaseRule.beginTx() )
        {
            rel = node.createRelationshipTo( databaseRule.createNode(), MyRelTypes.TEST );
            tx.success();
        }

        try ( Transaction tx = databaseRule.beginTx() )
        {
            // THEN
            assertEquals( rel, single( node.getRelationships() ) );
            tx.success();
        }
    }

    private int denseNodeThreshold( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver()
                .resolveDependency( Config.class ).get( GraphDatabaseSettings.dense_node_threshold );
    }

    private void deleteRelationshipsFromNode( Node root, int numberOfRelationships )
    {
        int deleted = 0;
        try ( ResourceIterator<Relationship> iterator = asResourceIterator( root.getRelationships().iterator() ) )
        {
            while ( iterator.hasNext() )
            {
                Relationship relationship = iterator.next();
                relationship.delete();
                deleted++;
                if ( deleted == numberOfRelationships )
                {
                    break;
                }
            }
        }
    }

    private void createRelationshipsOnNode( GraphDatabaseService db, Node root, int numberOfRelationships )
    {
        for ( int i = 0; i < numberOfRelationships; i++ )
        {
            root.createRelationshipTo( db.createNode(), RelationshipType.withName( "Type" + (i % 4) ) )
                    .setProperty( "" + i, i );

        }
    }

    private void createRelationshipsBetweenNodes( Node source, Node sink,
                                                  int numberOfRelationships )
    {
        for ( int i = 0; i < numberOfRelationships; i++ )
        {
            source.createRelationshipTo( sink, RelationshipType.withName( "Type" + (i % 4) ) )
                    .setProperty( "" + i, i );

        }
    }
}
