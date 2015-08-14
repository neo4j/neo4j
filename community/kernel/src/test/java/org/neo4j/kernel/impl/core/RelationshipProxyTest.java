/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.RelationshipProxy.RelationshipActions;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.single;

public class RelationshipProxyTest
{
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );
    private static final String PROPERTY_KEY = "PROPERTY_KEY";

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldBeAbleToReferToIdsBeyondMaxInt() throws Exception
    {
        // GIVEN
        RelationshipActions actions = mock( RelationshipActions.class );
        when( actions.newNodeProxy( anyLong() ) ).then( new Answer<Node>()
        {
            @Override
            public Node answer( InvocationOnMock invocation ) throws Throwable
            {
                return nodeWithId( (Long) invocation.getArguments()[0] );
            }
        } );
        when( actions.getRelationshipTypeById( anyInt() ) ).then( new Answer<RelationshipType>()
        {
            @Override
            public RelationshipType answer( InvocationOnMock invocation ) throws Throwable
            {
                return new RelationshipTypeToken( "whatever", (Integer) invocation.getArguments()[0] );
            }
        } );

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
    public void shouldThrowWhenSettingPropertyOnADeletedRelationship()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().createRelationshipTo( db.createNode(), TYPE ).setProperty( PROPERTY_KEY, "foo" );
            tx.success();
        }

        long relationshipId = -1;
        try ( Transaction ignored = db.beginTx() )
        {
            // When
            Relationship relationship = single( GlobalGraphOperations.at( db ).getAllRelationships() );
            relationshipId = relationship.getId();
            relationship.delete();
            relationship.setProperty( PROPERTY_KEY, "bar" );
            fail( "Setting property on a deleted relationship should not work" );
        }
        catch ( NotFoundException e )
        {
            // Then
            // exception is thrown - expected
        }
    }

    @Test
    public void shouldThrowWhenSettingPropertyOnARelationshipDeletedInSameTx()
    {
        long relationshipId = -1;
        try ( Transaction ignored = db.beginTx() )
        {
            // Given
            db.createNode().createRelationshipTo( db.createNode(), TYPE ).setProperty( PROPERTY_KEY, "foo" );

            // When
            Relationship relationship = single( GlobalGraphOperations.at( db ).getAllRelationships() );
            relationshipId = relationship.getId();
            relationship.delete();
            relationship.setProperty( PROPERTY_KEY, "bar" );
            fail( "Setting property on a deleted relationship should not work" );
        }
        catch ( NotFoundException e )
        {
            // Then
            // exception is thrown - expected
        }
    }

    private void verifyIds( RelationshipActions actions, long relationshipId, long nodeId1, int typeId, long nodeId2 )
    {
        RelationshipProxy proxy = new RelationshipProxy( actions, relationshipId, nodeId1, typeId, nodeId2 );
        assertEquals( relationshipId, proxy.getId() );
        // our mock above is known to return RelationshipTypeToken
        assertEquals( typeId, ((RelationshipTypeToken) proxy.getType()).id() );
        assertEquals( nodeId1, proxy.getStartNode().getId() );
        assertEquals( nodeId2, proxy.getEndNode().getId() );
        assertEquals( nodeId2, proxy.getOtherNode( nodeWithId( nodeId1 ) ).getId() );
        assertEquals( nodeId1, proxy.getOtherNode( nodeWithId( nodeId2 ) ).getId() );
    }

    private Node nodeWithId( long id )
    {
        Node node = mock( Node.class );
        when( node.getId() ).thenReturn( id );
        return node;
    }
}
