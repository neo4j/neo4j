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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.READ;
import static org.neo4j.internal.kernel.api.security.AuthSubject.ANONYMOUS;

public class ExplicitIndexTest
{
    private static final RelationshipType TYPE = RelationshipType.withName( "TYPE" );

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
    public void shouldThrowIllegalArgumentChangingTypeOfFieldOnNodeIndex()
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

    @Test
    public void shouldThrowIllegalArgumentChangingTypeOfFieldOnRelationshipIndex()
    {
        String indexName = "index";

        createRelationshipExplicitIndexWithSingleRelationship( db, indexName );

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
    public void relationshipIndexShouldBeAbleToReindexInSameTransaction()
    {
        // Create relationship and index
        Node startNode;
        Node endNode;
        Relationship relationship;
        RelationshipIndex index;
        try ( Transaction tx = db.beginTx() )
        {
            startNode = db.createNode();
            endNode = db.createNode();
            relationship = startNode.createRelationshipTo( endNode, TYPE );

            index = db.index().forRelationships( TYPE.name() );
            index.add( relationship, "key", new ValueContext( 1 ).indexNumeric() );

            tx.success();
        }

        // Verify
        assertTrue( "Find relationship by property", relationshipExistsByQuery( index, startNode, endNode, false ) );
        assertTrue( "Find relationship by property and start node", relationshipExistsByQuery( index, startNode, endNode, true ) );

        // Reindex
        try ( Transaction tx = db.beginTx() )
        {
            index.remove( relationship );
            index.add( relationship, "key", new ValueContext( 2 ).indexNumeric() );
            tx.success();
        }

        // Verify again
        assertTrue( "Find relationship by property", relationshipExistsByQuery( index, startNode, endNode, false ) );
        assertTrue( "Find relationship by property and start node", relationshipExistsByQuery( index, startNode, endNode, true ) );
    }

    @Test
    public void getSingleMustNotCloseStatementTwice()
    {
        // given
        String indexName = "index";
        long expected1;
        long expected2;
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node1, "key", "hej" );
            nodeIndex.add( node2, "key", "hejhej" );

            expected1 = node1.getId();
            expected2 = node2.getId();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( indexName );

            // when using getSingle this should not close statement for outer loop
            IndexHits<Node> hits = nodeIndex.query( "key", "hej" );
            while ( hits.hasNext() )
            {
                Node actual1 = hits.next();
                assertEquals( expected1, actual1.getId() );

                IndexHits<Node> hits2 = nodeIndex.query( "key", "hejhej" );
                Node actual2 = hits2.getSingle();
                assertEquals( expected2, actual2.getId() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldAllowReadTransactionToSkipDeletedNodes()
    {
        // given an indexed node
        String indexName = "index";
        Index<Node> nodeIndex;
        Node node;
        String key = "key";
        String value = "value";
        try ( Transaction tx = db.beginTx() )
        {
            nodeIndex = db.index().forNodes( indexName );
            node = db.createNode();
            nodeIndex.add( node, key, value );
            tx.success();
        }
        // delete the node, but keep it in the index
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();
            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTransaction( explicit, new SecurityContext( ANONYMOUS, READ ) ) )
        {
            IndexHits<Node> hits = nodeIndex.get( key, value );
            // then
            assertNull( hits.getSingle() );
        }
        // also the fact that a read-only tx can do this w/o running into permission violation is good
    }

    @Test
    public void shouldAllowReadTransactionToSkipDeletedRelationships()
    {
        // given an indexed relationship
        String indexName = "index";
        Index<Relationship> relationshipIndex;
        Relationship relationship;
        String key = "key";
        String value = "value";
        try ( Transaction tx = db.beginTx() )
        {
            relationshipIndex = db.index().forRelationships( indexName );
            relationship = db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            relationshipIndex.add( relationship, key, value );
            tx.success();
        }
        // delete the relationship, but keep it in the index
        try ( Transaction tx = db.beginTx() )
        {
            relationship.delete();
            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTransaction( explicit, new SecurityContext( ANONYMOUS, READ ) ) )
        {
            IndexHits<Relationship> hits = relationshipIndex.get( key, value );
            // then
            assertNull( hits.getSingle() );
        }
        // also the fact that a read-only tx can do this w/o running into permission violation is good
    }

    private boolean relationshipExistsByQuery( RelationshipIndex index, Node startNode, Node endNode, boolean specifyStartNode )
    {
        boolean found = false;

        try ( Transaction tx = db.beginTx(); IndexHits<Relationship> query = index
                .query( "key", QueryContext.numericRange( "key", 0, 3 ), specifyStartNode ? startNode : null, null ) )
        {
            for ( Relationship relationship : query )
            {
                if ( relationship.getStartNodeId() == startNode.getId() && relationship.getEndNodeId() == endNode.getId() )
                {
                    found = true;
                    break;
                }
            }

            tx.success();
        }
        return found;
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
