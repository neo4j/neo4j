/**
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

import org.junit.Test;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.util.RelIdArray;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        Throwable exceptionCaught = null;

        // Given something tries to load relationships, throw InvalidRecordException
        when( nodeManager.getMoreRelationships( any( NodeImpl.class ) ) ).thenThrow( new InvalidRecordException(
                "LURING!" ) );

        // When
        try
        {
            nodeImpl.getAllRelationships( nodeManager, RelIdArray.DirectionWrapper.BOTH );
        }
        catch ( Throwable e )
        {
            exceptionCaught = e;
        }

        // Then
        assertThat( exceptionCaught, not( nullValue() ) );
        assertThat( exceptionCaught, is( instanceOf( NotFoundException.class ) ) );
    }

    @Test
    public void shouldThrowNotFoundWhenIteratingOverDeletedRelationship() throws Exception
    {
        // Given
        NodeImpl fromNode = new NodeImpl( 1337l, false );
        NodeManager nodeManager = mock( NodeManager.class );
        Throwable exceptionCaught = null;

        // This makes fromNode think there are more relationships to be loaded
        fromNode.setRelChainPosition( 1337l );

        // This makes nodeManager pretend that relationships have been deleted
        when( nodeManager.getMoreRelationships( any( NodeImpl.class ) ) ).thenThrow( new InvalidRecordException(
                "LURING!" ) );


        // When
        try
        {
            fromNode.getMoreRelationships( nodeManager );
        }
        catch ( Throwable e )
        {
            exceptionCaught = e;
        }

        // Then
        assertThat( exceptionCaught, not( nullValue() ) );
        assertThat( exceptionCaught, is( instanceOf( NotFoundException.class ) ) );
    }

}
