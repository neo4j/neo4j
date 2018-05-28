/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LegacyIndexTest
{
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void removalOfNodeIndexDoesNotInfluenceRelationshipIndexWithSameName()
    {
        String indexName = "index";

        createNodeLegacyIndexWithSingleNode( db, indexName );
        createRelationshipLegacyIndexWithSingleRelationship( db, indexName );

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

        createNodeLegacyIndexWithSingleNode( db, indexName );
        createRelationshipLegacyIndexWithSingleRelationship( db, indexName );

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
    public void shouldThrowIllegalArgumentChangingTypeOfFieldOnNodeIndex()
    {
        String indexName = "index";

        createNodeLegacyIndexWithSingleNode( db, indexName );

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

    @Test
    public void shouldBeAbleToAddNodesAfterRemovalOfKey()
    {
        String indexName = "index";
        long nodeId;
        //add two keys and delete one of them
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node, "key", "hej" );
            nodeIndex.add( node, "keydelete", "hej" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.remove( db.getNodeById( nodeId ), "keydelete" );
            tx.success();
        }

        db.shutdownAndKeepStore();
        db.getGraphDatabaseAPI();

        //should be able to add more stuff to the index
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node, "key", "hej" );
            tx.success();
        }
    }

    @Test
    public void indexContentsShouldStillBeOrderedAfterRemovalOfKey()
    {
        String indexName = "index";
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( indexName );
            tx.success();
        }

        long delete;
        long first;
        long second;
        long third;
        long fourth;
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            Node node = db.createNode();
            delete = node.getId();
            nodeIndex.add( node, "keydelte", "delete" );
            node = db.createNode();
            second = node.getId();
            nodeIndex.add( node, "key", ValueContext.numeric( 2 ) );
            nodeIndex.add( node, "keydelte", "delete" );

            node = db.createNode();
            fourth = node.getId();
            nodeIndex.add( node, "key", ValueContext.numeric( 4 ) );
            nodeIndex.add( node, "keydelte", "delete" );

            node = db.createNode();
            first = node.getId();
            nodeIndex.add( node, "key", ValueContext.numeric( 1 ) );
            nodeIndex.add( node, "keydelte", "delete" );

            node = db.createNode();
            third = node.getId();
            nodeIndex.add( node, "key", ValueContext.numeric( 3 ) );
            nodeIndex.add( node, "keydelte", "delete" );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            IndexHits<Node> query = nodeIndex.query( "key", QueryContext.numericRange( "key", 2, 3 ) );
            assertEquals( 2, query.size() );
            query.forEachRemaining( node -> assertTrue( node.getId() == second || node.getId() == third ) );
        }

        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.remove( db.getNodeById( delete ), "keydelete" );
            nodeIndex.remove( db.getNodeById( first ), "keydelete" );
            nodeIndex.remove( db.getNodeById( second ), "keydelete" );
            nodeIndex.remove( db.getNodeById( third ), "keydelete" );
            nodeIndex.remove( db.getNodeById( fourth ), "keydelete" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            IndexHits<Node> query = nodeIndex.query( "key", QueryContext.numericRange( "key", 2, 3 ) );
            assertEquals( 2, query.size() );
            query.forEachRemaining( node -> assertTrue( node.getId() == second || node.getId() == third ) );
        }
    }

    @Test
    public void shouldThrowIllegalArgumentChangingTypeOfFieldOnRelationshipIndex()
    {
        String indexName = "index";

        createRelationshipLegacyIndexWithSingleRelationship( db, indexName );

        long relId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Relationship rel = node.createRelationshipTo( node, TYPE );
            relId = rel.getId();
            RelationshipIndex index = db.index().forRelationships( indexName );
            index.add( rel, "key", "otherValue" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            RelationshipIndex index = db.index().forRelationships( indexName );
            index.remove( db.getRelationshipById( relId ), "key" );
            tx.success();
        }

        expectedException.expect( IllegalArgumentException.class );
        try ( Transaction tx = db.beginTx() )
        {
            RelationshipIndex index = db.index().forRelationships( indexName );
            index.add( db.getRelationshipById( relId ), "key", ValueContext.numeric( 52 ) );
            tx.success();
        }
    }

    private static void createNodeLegacyIndexWithSingleNode( GraphDatabaseService db, String indexName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node, "key", System.currentTimeMillis() );
            tx.success();
        }
    }

    private static void createRelationshipLegacyIndexWithSingleRelationship( GraphDatabaseService db, String indexName )
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
        return index.query( "_id_:*" ).size();
    }
}
