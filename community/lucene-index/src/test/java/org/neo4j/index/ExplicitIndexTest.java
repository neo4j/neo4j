/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ExplicitIndexTest
{
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void removalOfNodeIndexDoesNotInfluenceRelationshipIndexWithSameName()
    {
        String indexName = "index";

        createNodeExplicitIndexWithSingleNode( db, indexName );
        createRelationshipExplicitIndexWithSingleRelationship( db, indexName );

        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.createNode().createRelationshipTo( db.createNode(), TYPE );
            Index<Relationship> relationshipIndex = db.index().forRelationships( indexName );
            relationshipIndex.add( relationship, "key", "otherValue" );

            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.delete();

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.index().existsForNodes( indexName ) );
            Index<Relationship> relationshipIndex = db.index().forRelationships( indexName );
            assertEquals( 2, sizeOf( relationshipIndex ) );
            tx.success();
        }
    }

    @Test
    public void removalOfRelationshipIndexDoesNotInfluenceNodeIndexWithSameName()
    {
        String indexName = "index";

        createNodeExplicitIndexWithSingleNode( db, indexName );
        createRelationshipExplicitIndexWithSingleRelationship( db, indexName );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node, "key", "otherValue" );

            Index<Relationship> relationshipIndex = db.index().forRelationships( indexName );
            relationshipIndex.delete();

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.index().existsForRelationships( indexName ) );
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            assertEquals( 2, sizeOf( nodeIndex ) );
            tx.success();
        }
    }

    @Test
    public void shouldThrowIllegalArgumentChangingTypeOfField()
    {
        String indexName = "index";

        createNodeExplicitIndexWithSingleNode( db, indexName );

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node, "key", "otherValue" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.remove( db.getNodeById( nodeId ), "key" );
            tx.success();
        }

        expectedException.expect( IllegalArgumentException.class );
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( db.getNodeById( nodeId ), "key", ValueContext.numeric( 52 ) );
            tx.success();
        }
    }

    private static void createNodeExplicitIndexWithSingleNode( GraphDatabaseService db, String indexName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node, "key", System.currentTimeMillis() );
            tx.success();
        }
    }

    private static void createRelationshipExplicitIndexWithSingleRelationship( GraphDatabaseService db, String indexName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.createNode().createRelationshipTo( db.createNode(), TYPE );
            Index<Relationship> relationshipIndexIndex = db.index().forRelationships( indexName );
            relationshipIndexIndex.add( relationship, "key", System.currentTimeMillis() );
            tx.success();
        }
    }

    private static int sizeOf( Index<?> index )
    {
        try ( IndexHits<?> indexHits = index.query( "_id_:*" ) )
        {
            return indexHits.size();
        }
    }
}
