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

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;

@ImpermanentDbmsExtension
class DenseNodeIT
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void testBringingNodeOverDenseThresholdIsConsistent()
    {
        // GIVEN
        Node root;
        try ( Transaction tx = db.beginTx() )
        {
            root = db.createNode();
            createRelationshipsOnNode( db, root, 40 );
            tx.commit();
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
            tx.commit();
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
            tx.commit();
        }
    }

    @Test
    void deletingRelationshipsFromDenseNodeIsConsistent()
    {
        // GIVEN
        Node root;
        try ( Transaction tx = db.beginTx() )
        {
            root = db.createNode();
            createRelationshipsOnNode( db, root, 100 );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            deleteRelationshipsFromNode( root, 80 );

            assertEquals( 20, root.getDegree() );
            assertEquals( 20, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 20, root.getDegree() );
            assertEquals( 20, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );
            tx.commit();
        }
    }

    @Test
    void movingBilaterallyOfTheDenseNodeThresholdIsConsistent()
    {
        // GIVEN
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

            tx.commit();
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 20, root.getDegree() );
            assertEquals( 20, root.getDegree( Direction.OUTGOING ) );
            assertEquals( 0, root.getDegree( Direction.INCOMING ) );
            tx.commit();
        }
    }

    @Test
    void testBringingTwoConnectedNodesOverDenseThresholdIsConsistent()
    {
        // GIVEN
        Node source;
        Node sink;
        try ( Transaction tx = db.beginTx() )
        {
            source = db.createNode();
            sink = db.createNode();
            createRelationshipsBetweenNodes( source, sink, 40 );
            tx.commit();
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
            tx.commit();
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
            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToCreateRelationshipsInEmptyDenseNode()
    {
        // GIVEN
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            createRelationshipsBetweenNodes( node, db.createNode(), denseNodeThreshold( db ) + 1 );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            node.getRelationships().forEach( Relationship::delete );
            tx.commit();
        }

        // WHEN
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            rel = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            // THEN
            assertEquals( rel, single( node.getRelationships() ) );
            tx.commit();
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
