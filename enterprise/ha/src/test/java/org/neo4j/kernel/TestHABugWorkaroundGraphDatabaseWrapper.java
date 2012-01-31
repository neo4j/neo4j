/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.TestHABugWorkaroundGraphDatabaseWrapper.RelationshipOtherNodeIterableWrapper.otherNodes;

public class TestHABugWorkaroundGraphDatabaseWrapper
{
    private static final TargetDirectory target = TargetDirectory
        .forTest( TestHABugWorkaroundGraphDatabaseWrapper.class );
    private static GraphDatabaseService graphdb;

    @BeforeClass
    public static void startGraphDB() throws Exception
    {
        graphdb = new HighlyAvailableGraphDatabase( new EmbeddedGraphDatabase( target.graphDbDir( true )
            .getAbsolutePath() ) );
    }

    @AfterClass
    public static void stopGraphDB() throws Exception
    {
        if ( graphdb != null ) graphdb.shutdown();
        graphdb = null;
    }

    @Test
    public void canCreateNode()
    {
        createNode();
    }

    @Test( expected = NotFoundException.class )
    public void canRemoveNode()
    {
        Node node = createNode();
        long id = node.getId();
        Transaction tx = graphdb.beginTx();
        try
        {
            node.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        graphdb.getNodeById( id );
    }

    @Test
    public void canCreateRelationship()
    {
        Node node1 = createNode();
        Node node2 = createNode();

        Relationship rel = createRelationship( node1, node2, TestTypes.TEST );
        assertEquals( node1, rel.getStartNode() );
        assertEquals( node2, rel.getEndNode() );
        assertEquals( TestTypes.TEST.name(), rel.getType().name() );
        assertArrayEquals( new Node[] { node1, node2 }, rel.getNodes() );
        assertTrue( rel.isType( TestTypes.TEST ) );
    }

    @Test( expected = NotFoundException.class )
    public void canDeleteRelationship()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship rel;
        rel = createRelationship( node1, node2, TestTypes.TEST );
        long id = rel.getId();
        Transaction tx = graphdb.beginTx();
        try
        {
            rel.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        graphdb.getRelationshipById( id );
    }

    @Test
    public void canOperateWithNodeProperty() throws Exception
    {
        canOperateWithProperty( EntityType.NODE );
    }

    @Test
    public void canOperateWithRelationshipProperty() throws Exception
    {
        canOperateWithProperty( EntityType.REL );
    }

    @Test
    public void canReadRelationships()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        Node node3 = createNode();
        createRelationship( node1, node2, TestTypes.TEST );
        createRelationship( node3, node1, TestTypes.FOO );

        assertContainsAll( otherNodes( node1, node1.getRelationships() ), node2, node3 );
        assertContainsAll( otherNodes( node1, node1.getRelationships( Direction.OUTGOING ) ), node2 );
        assertContainsAll( otherNodes( node1, node1.getRelationships( TestTypes.FOO ) ), node3 );
        assertContainsAll( otherNodes( node1, node1.getRelationships( Direction.OUTGOING, TestTypes.FOO, TestTypes.TEST ) ), node2 );
        assertContainsAll( otherNodes( node1, node1.getRelationships( TestTypes.FOO, Direction.INCOMING ) ), node3 );
        assertEquals( node2, node1.getSingleRelationship( TestTypes.TEST, Direction.OUTGOING ).getOtherNode( node1 ) );
        assertTrue( node1.hasRelationship( Direction.OUTGOING ) );
        assertTrue( node1.hasRelationship( Direction.INCOMING, TestTypes.FOO ) );
        assertTrue( node1.hasRelationship( TestTypes.FOO ) );
        assertTrue( node1.hasRelationship( TestTypes.TEST, Direction.OUTGOING ) );
        assertTrue( node1.hasRelationship() );
        assertFalse( node1.hasRelationship( TestTypes.TEST, Direction.INCOMING ) );
        assertNotNull( node2.getSingleRelationship( TestTypes.TEST, Direction.BOTH ) );
        assertNull( node2.getSingleRelationship( TestTypes.FOO, Direction.BOTH ) );
    }

    @Test
    public void canUseNodeIndex()
    {
        Node node = createNode();
        Index<Node> nodeIndex = graphdb.index().forNodes( "nodeIndex" );

        indexAdd( nodeIndex, node, "key", "value" );
        assertEquals( node, nodeIndex.get( "key", "value" ).getSingle() );
        assertEquals( node, nodeIndex.query( "key:value" ).getSingle() );
        assertEquals( node, nodeIndex.query( "key", "value" ).getSingle() );
        assertContainsAll( nodeIndex.get( "key", "value" ), node );
        assertEquals( 1, nodeIndex.get( "key", "value" ).size() );
        indexRemove( nodeIndex, node );
        assertNull( nodeIndex.get( "key", "value" ).getSingle() );

        indexAdd( nodeIndex, node, "key", "value" );
        assertEquals( node, nodeIndex.get( "key", "value" ).getSingle() );
        indexRemove( nodeIndex, node, "key" );
        assertNull( nodeIndex.get( "key", "value" ).getSingle() );

        indexAdd( nodeIndex, node, "key", "value" );
        assertEquals( node, nodeIndex.get( "key", "value" ).getSingle() );
        indexRemove( nodeIndex, node, "key", "value" );
        assertNull( nodeIndex.get( "key", "value" ).getSingle() );

        assertEquals( "nodeIndex", nodeIndex.getName() );
        assertEquals( Node.class, nodeIndex.getEntityType() );
    }

    @Test
    public void canUseRelationshipIndex()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        Node node3 = createNode();
        Relationship rel = createRelationship( node1, node2, TestTypes.TEST );
        RelationshipIndex relIndex = graphdb.index().forRelationships( "relIndex" );

        indexAdd( relIndex, rel, "key", "value" );
        assertEquals( rel, relIndex.get( "key", "value" ).getSingle() );
        assertEquals( rel, relIndex.get( "key", "value", node1, node2 ).getSingle() );
        assertNull( relIndex.get( "key", "value", node1, node3 ).getSingle() );
        assertEquals( rel, relIndex.query( "key:value" ).getSingle() );
        assertEquals( rel, relIndex.query( "key", "value" ).getSingle() );
        assertEquals( rel, relIndex.query( "key", "value", node1, node2 ).getSingle() );
        assertEquals( rel, relIndex.query( "key:value", node1, node2 ).getSingle() );
        assertNull( relIndex.query( "key", "value", node1, node3 ).getSingle() );
        assertContainsAll( relIndex.get( "key", "value" ), rel );
        assertEquals( 1, relIndex.get( "key", "value" ).size() );
        indexRemove( relIndex, rel );
        assertNull( relIndex.get( "key", "value" ).getSingle() );

        indexAdd( relIndex, rel, "key", "value" );
        assertEquals( rel, relIndex.get( "key", "value" ).getSingle() );
        indexRemove( relIndex, rel, "key" );
        assertNull( relIndex.get( "key", "value" ).getSingle() );

        indexAdd( relIndex, rel, "key", "value" );
        assertEquals( rel, relIndex.get( "key", "value" ).getSingle() );
        indexRemove( relIndex, rel, "key", "value" );
        assertNull( relIndex.get( "key", "value" ).getSingle() );

        assertEquals( "relIndex", relIndex.getName() );
        assertEquals( Relationship.class, relIndex.getEntityType() );
    }

    private <T extends PropertyContainer> void indexRemove( Index<T> nodeIndex, T node )
    {
        Transaction tx;
        tx = graphdb.beginTx();
        try
        {
            nodeIndex.remove( node );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private <T extends PropertyContainer> void indexRemove( Index<T> nodeIndex, T node, String key )
    {
        Transaction tx;
        tx = graphdb.beginTx();
        try
        {
            nodeIndex.remove( node, key );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
    private <T extends PropertyContainer> void indexRemove( Index<T> nodeIndex, T node, String key, Object value )
    {
        Transaction tx;
        tx = graphdb.beginTx();
        try
        {
            nodeIndex.remove( node, key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private <T extends PropertyContainer> void indexAdd( Index<T> nodeIndex, T node, String key, String value )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            nodeIndex.add( node, key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public Node createNode()
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Node node = graphdb.createNode();
            assertNotNull( node );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }

    private Relationship createRelationship( Node from, Node to, TestTypes type )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Relationship rel = from.createRelationshipTo( to, type );
            tx.success();
            return rel;
        }
        finally
        {
            tx.finish();
        }
    }

    public Relationship createRelationship()
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Relationship rel = createNode().createRelationshipTo( createNode(), TestTypes.TEST );
            assertNotNull( rel );
            tx.success();
            return rel;
        }
        finally
        {
            tx.finish();
        }
    }

    private void canOperateWithProperty( EntityType entityType ) throws Exception
    {
        PropertyContainer entity;
        Transaction tx = graphdb.beginTx();
        try
        {
            entity = entityType.create( this );
            assertFalse( entity.hasProperty( "key" ) );
            assertNull( entity.getProperty( "key", null ) );
            assertContainsAll( entity.getPropertyKeys() );
            assertContainsAll( entity.getPropertyValues() );
            entity.setProperty( "key", "value" );
            assertTrue( entity.hasProperty( "key" ) );
            assertEquals( "value", entity.getProperty( "key" ) );
            assertEquals( "value", entity.getProperty( "key", null ) );
            assertNull( entity.getProperty( "foo", null ) );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
        assertTrue( entity.hasProperty( "key" ) );
        assertEquals( "value", entity.getProperty( "key" ) );
        assertEquals( "value", entity.getProperty( "key", null ) );
        assertContainsAll( entity.getPropertyKeys(), "key" );
        assertContainsAll( entity.getPropertyValues(), "value" );
        tx = graphdb.beginTx();
        try
        {
            assertEquals( "value", entity.removeProperty( "key" ) );
            assertFalse( entity.hasProperty( "key" ) );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
        assertFalse( entity.hasProperty( "key" ) );
        assertNull( entity.getProperty( "key", null ) );
        assertContainsAll( entity.getPropertyKeys() );
        assertContainsAll( entity.getPropertyValues() );
    }

    private <T> void assertContainsAll( Iterable<T> actual, T... values )
    {
        Set<T> expected = new HashSet<T>( Arrays.asList( values ) );
        for ( T value : actual )
        {
            assertTrue( String.format( "Unexpected value <%s>", value ), expected.remove( value ) );
        }
        assertTrue( String.format( "Missing values %s", expected ), expected.isEmpty() );
    }

    enum EntityType
    {
        NODE
            {
                @Override
                PropertyContainer create( TestHABugWorkaroundGraphDatabaseWrapper test )
                {
                    return test.createNode();
                }
            },
        REL
            {
                @Override
                PropertyContainer create( TestHABugWorkaroundGraphDatabaseWrapper test )
                {
                    return test.createRelationship();
                }
            };

        abstract PropertyContainer create( TestHABugWorkaroundGraphDatabaseWrapper test );
    }

    enum TestTypes implements RelationshipType
    {
        TEST, FOO
    }

    public static class RelationshipOtherNodeIterableWrapper extends IterableWrapper<Node, Relationship>
    {
        private final Node node;

        public RelationshipOtherNodeIterableWrapper( Node node, Iterable<Relationship> relationships )
        {
            super( relationships );
            this.node = node;
        }

        @Override
        protected Node underlyingObjectToObject( Relationship relationship )
        {
            return relationship.getOtherNode( node );
        }

        public static RelationshipOtherNodeIterableWrapper otherNodes( Node node, Iterable<Relationship> relationships )
        {
            return new RelationshipOtherNodeIterableWrapper( node, relationships );
        }
    }
}
