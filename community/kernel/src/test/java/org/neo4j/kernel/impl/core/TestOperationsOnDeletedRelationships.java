/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestOperationsOnDeletedRelationships
{
    // Should it really do this? Wouldn't it be better if we could recover from a a relationship suddenly
    // missing in the chain? Perhaps that is really hard to do though.
    @Test
    public void shouldThrowNotFoundOnGetAllRelationshipsWhenRelationshipConcurrentlyDeleted() throws Exception
    {
        // Given
        NodeImpl nodeImpl = new NodeImpl( 1337l, false );
        NodeManager nodeManager = mock( NodeManager.class );

        // Given something tries to load relationships, throw InvalidRecordException
        when( nodeManager.getRelationshipChainPosition( any( NodeImpl.class ) ) ).thenReturn(
                new SingleChainPosition( 1 ) );
        when( nodeManager.getMoreRelationships( any( NodeImpl.class ), any( DirectionWrapper.class ),
                any( int[].class ) ) ).thenThrow( new InvalidRecordException( "LURING!" ) );

        // When
        try
        {
            nodeImpl.getAllRelationships( nodeManager, RelIdArray.DirectionWrapper.BOTH );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        {
            // Then
        }
    }

    @Test
    public void shouldThrowNotFoundWhenIteratingOverDeletedRelationship() throws Exception
    {
        // Given
        NodeImpl fromNode = new NodeImpl( 1337l, false );
        NodeManager nodeManager = mock( NodeManager.class );

        // This makes nodeManager pretend that relationships have been deleted
        when( nodeManager.getMoreRelationships( any( NodeImpl.class ), any( DirectionWrapper.class ),
                any( int[].class ) ) ).thenThrow( new InvalidRecordException( "LURING!" ) );
        fromNode.setRelChainPosition( new SingleChainPosition( 1 ) );

        // When
        try
        {
            fromNode.getMoreRelationships( nodeManager, DirectionWrapper.BOTH, new int[0] );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        {
            // Then
        }
    }
}
