/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeProxySingleRelationshipTest
{
    private static final long REL_ID = 1;
    private static final RelationshipType loves = DynamicRelationshipType.withName( "LOVES" );

    /**
     * This behaviour is a workaround until we have proper concurrency support in the kernel.
     * It fixes a problem at the lower levels whereby a source containing a relationship from disk
     * gets merged with a add COW map containing the same relationship in an uncommitted transaction
     * giving unhelpful duplicates at the API level. One day this unit test can be removed,
     * but that day is not today.
     */
    @Test
    public void shouldQuietlyIgnoreSingleDuplicateEntryWhenGetSingleRelationshipCalled() throws Exception
    {
        // given
        NodeProxy nodeImpl = mockNodeWithRels( REL_ID, REL_ID );

        // when
        Relationship singleRelationship = nodeImpl.getSingleRelationship( loves, Direction.OUTGOING );

        // then
        assertNotNull( singleRelationship );
    }

    @Test
    public void shouldThrowExceptionIfMultipleDifferentEntries() throws Exception
    {
        // given
        NodeProxy node = mockNodeWithRels( REL_ID, REL_ID + 1 );

        // when
        try
        {
            node.getSingleRelationship( loves, Direction.OUTGOING );
            fail("expected exception");
        }
        catch ( NotFoundException expected )
        {
        }
    }

    @Test
    public void shouldThrowExceptionIfMultipleDifferentEntriesWithTwoOfThemBeingIdentical() throws Exception
    {
        // given
        NodeProxy node = mockNodeWithRels( REL_ID, REL_ID, REL_ID + 1, REL_ID + 1);

        // when
        try
        {
            node.getSingleRelationship( loves, Direction.OUTGOING );
            fail();
        }
        catch ( NotFoundException expected )
        {
        }
    }

    private NodeProxy mockNodeWithRels( final long ... relIds) throws EntityNotFoundException
    {
        NodeProxy.NodeActions nodeActions = mock( NodeProxy.NodeActions.class );
        final RelationshipProxy.RelationshipActions relActions = mock( RelationshipProxy.RelationshipActions.class );
        when( nodeActions.newRelationshipProxy( anyLong() ) ).thenAnswer( new Answer<RelationshipProxy>()
        {
            @Override
            public RelationshipProxy answer( InvocationOnMock invocation ) throws Throwable
            {
                return new RelationshipProxy( relActions, (Long)invocation.getArguments()[0] );
            }
        } );
        when( nodeActions.newRelationshipProxy( anyLong(), anyLong(), anyInt(), anyLong() ) ).then(
                new Answer<Relationship>()
                {
                    @Override
                    public Relationship answer( InvocationOnMock invocation ) throws Throwable
                    {
                        Long id = (Long) invocation.getArguments()[0];
                        Long startNode = (Long) invocation.getArguments()[1];
                        Integer type = (Integer) invocation.getArguments()[2];
                        Long endNode = (Long) invocation.getArguments()[3];
                        return new RelationshipProxy( relActions, id, startNode, type, endNode );
                    }
                } );

        GraphDatabaseService gds = mock( GraphDatabaseService.class );

        when(gds.getRelationshipById( REL_ID )).thenReturn( mock( Relationship.class ) );
        when(gds.getRelationshipById( REL_ID + 1)).thenReturn( mock(Relationship.class) );
        when( nodeActions.getGraphDatabase() ).thenReturn( gds );

        NodeProxy nodeImpl = new NodeProxy( nodeActions, 1 );

        Statement stmt = mock( Statement.class );
        ReadOperations readOps = mock( ReadOperations.class );

        when( stmt.readOperations() ).thenReturn( readOps );
        when( nodeActions.statement() ).thenReturn( stmt );
        when( readOps.relationshipTypeGetForName( loves.name() ) ).thenReturn( 2 );

        when( readOps.nodeGetRelationships( eq( 1L ), eq( Direction.OUTGOING ), eq( 2 ) ) ).thenAnswer( new Answer<RelationshipIterator>()
        {
            @Override
            public RelationshipIterator answer( InvocationOnMock invocation ) throws Throwable
            {
                return new RelationshipIterator.BaseIterator()
                {
                    int pos;
                    long relId;

                    @Override
                    protected boolean fetchNext()
                    {
                        if ( pos < relIds.length )
                        {
                            relId = relIds[pos++];
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }

                    @Override
                    public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                            RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
                    {
                        visitor.visit( relId, 2, 1, 10 * relId + 2 );
                        return false;
                    }
                };
            }
        } );
        return nodeImpl;
    }
}
