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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.RelationshipType.withName;

public class RelationshipProxyTest extends PropertyContainerProxyTest
{
    @Override
    protected long createPropertyContainer()
    {
        return db.createNode().createRelationshipTo( db.createNode(), withName( "FOO" ) ).getId();
    }

    @Override
    protected PropertyContainer lookupPropertyContainer( long id )
    {
        return db.getRelationshipById( id );
    }

    @Test
    public void shouldBeAbleToReferToIdsBeyondMaxInt()
    {
        // GIVEN
        EmbeddedProxySPI actions = mock( EmbeddedProxySPI.class );
        when( actions.newNodeProxy( anyLong() ) ).then(
                invocation -> nodeWithId( invocation.getArgument( 0 ) ) );
        when( actions.getRelationshipTypeById( anyInt() ) ).then(
                invocation -> new RelationshipTypeToken( "whatever", invocation.getArgument( 0 ) ) );

        long[] ids = new long[]{
                1437589437,
                2047587483,
                2147496246L,
                2147342921,
                3276473721L,
                4762746373L,
                57587348738L,
                59892898932L
        };
        int[] types = new int[]{
                0,
                10,
                101,
                3024,
                20123,
                45008
        };

        // WHEN/THEN
        for ( int i = 0; i < ids.length - 2; i++ )
        {
            long id = ids[i];
            long nodeId1 = ids[i + 1];
            long nodeId2 = ids[i + 2];
            int type = types[i];
            verifyIds( actions, id, nodeId1, type, nodeId2 );
            verifyIds( actions, id, nodeId2, type, nodeId1 );
        }
    }

    @Test
    public void shouldPrintCypherEsqueRelationshipToString()
    {
        // GIVEN
        Node start;
        Node end;
        RelationshipType type = RelationshipType.withName( "NICE" );
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            // GIVEN
            start = db.createNode();
            end = db.createNode();
            relationship = start.createRelationshipTo( end, type );
            tx.success();

            // WHEN
            String toString = relationship.toString();

            // THEN
            assertEquals( "(" + start.getId() + ")-[" + type + "," + relationship.getId() + "]->(" + end.getId() + ")",
                    toString );
        }
    }

    @Test
    public void createDropRelationshipLongStringProperty()
    {
        Label markerLabel = Label.label( "marker" );
        String testPropertyKey = "testProperty";
        String propertyValue = RandomStringUtils.randomAscii( 255 );

        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode( markerLabel );
            Node end = db.createNode( markerLabel );
            Relationship relationship = start.createRelationshipTo( end, withName( "type" ) );
            relationship.setProperty( testPropertyKey, propertyValue );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.getRelationshipById( 0 );
            assertEquals( propertyValue, relationship.getProperty( testPropertyKey ) );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.getRelationshipById( 0 );
            relationship.removeProperty( testPropertyKey );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.getRelationshipById( 0 );
            assertFalse( relationship.hasProperty( testPropertyKey ) );
            tx.success();
        }
    }

    @Test
    public void createDropRelationshipLongArrayProperty()
    {
        Label markerLabel = Label.label( "marker" );
        String testPropertyKey = "testProperty";
        byte[] propertyValue = RandomUtils.nextBytes( 1024 );

        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode( markerLabel );
            Node end = db.createNode( markerLabel );
            Relationship relationship = start.createRelationshipTo( end, withName( "type" ) );
            relationship.setProperty( testPropertyKey, propertyValue );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.getRelationshipById( 0 );
            assertArrayEquals( propertyValue, (byte[]) relationship.getProperty( testPropertyKey ) );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.getRelationshipById( 0 );
            relationship.removeProperty( testPropertyKey );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.getRelationshipById( 0 );
            assertFalse( relationship.hasProperty( testPropertyKey ) );
            tx.success();
        }
    }

    @Test
    public void shouldBeAbleToForceTypeChangeOfProperty()
    {
        // Given
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            relationship = db.createNode().createRelationshipTo( db.createNode(), withName( "R" ) );
            relationship.setProperty( "prop", 1337 );
            tx.success();
        }

        // When
        try ( Transaction tx = db.beginTx() )
        {
            relationship.setProperty( "prop", 1337.0 );
            tx.success();
        }

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            assertThat( relationship.getProperty( "prop" ), instanceOf( Double.class ) );
        }
    }

    private void verifyIds( EmbeddedProxySPI actions, long relationshipId, long nodeId1, int typeId, long nodeId2 )
    {
        RelationshipProxy proxy = new RelationshipProxy( actions, relationshipId, nodeId1, typeId, nodeId2 );
        assertEquals( relationshipId, proxy.getId() );
        // our mock above is known to return RelationshipTypeToken
        assertEquals( typeId, ((RelationshipTypeToken) proxy.getType()).id() );
        assertEquals( nodeId1, proxy.getStartNode().getId() );
        assertEquals( nodeId1, proxy.getStartNodeId() );
        assertEquals( nodeId2, proxy.getEndNode().getId() );
        assertEquals( nodeId2, proxy.getEndNodeId() );
        assertEquals( nodeId2, proxy.getOtherNode( nodeWithId( nodeId1 ) ).getId() );
        assertEquals( nodeId2, proxy.getOtherNodeId( nodeId1 ) );
        assertEquals( nodeId1, proxy.getOtherNode( nodeWithId( nodeId2 ) ).getId() );
        assertEquals( nodeId1, proxy.getOtherNodeId( nodeId2 ) );
    }

    private Node nodeWithId( long id )
    {
        NodeProxy node = mock( NodeProxy.class );
        when( node.getId() ).thenReturn( id );
        return node;
    }
}
