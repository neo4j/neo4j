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
package org.neo4j.cypher.internal.codegen;

import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.ReadOperations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.cypher.internal.codegen.CompiledExpandUtils.connectingRelationships;

public class CompiledExpandUtilsTest
{
    @Test
    public void shouldUseGivenOrderIfItHasLowerDegree() throws EntityNotFoundException
    {
        // GIVEN
        ReadOperations readOperations = mock( ReadOperations.class );
        Read read = mock( Read.class );
        NodeCursor nodeCursor = mock( NodeCursor.class );
        when( readOperations.nodeGetDegree( 1L, Direction.OUTGOING ) ).thenReturn( 1 );
        when( readOperations.nodeGetDegree( 2L, Direction.INCOMING ) ).thenReturn( 3 );

        // WHEN
        connectingRelationships( readOperations, read, mock( CursorFactory.class ), nodeCursor, 1L, Direction.OUTGOING, 2L );

        // THEN
        verify( read, times( 1 ) ).singleNode( 1L, nodeCursor);
    }

    @Test
    public void shouldSwitchOrderIfItHasLowerDegree() throws EntityNotFoundException
    {
        // GIVEN
        ReadOperations readOperations = mock( ReadOperations.class );
        Read read = mock( Read.class );
        NodeCursor nodeCursor = mock( NodeCursor.class );

        when( readOperations.nodeGetDegree( 1L, Direction.OUTGOING ) ).thenReturn( 3 );
        when( readOperations.nodeGetDegree( 2L, Direction.INCOMING ) ).thenReturn( 1 );

        // WHEN
        connectingRelationships( readOperations, read, mock( CursorFactory.class ), nodeCursor, 1L, Direction.OUTGOING, 2L );

        // THEN
        verify( read, times( 1 ) ).singleNode( 2L, nodeCursor );
    }

    @Test
    public void shouldUseGivenOrderIfItHasLowerDegreeWithTypes() throws EntityNotFoundException
    {
        // GIVEN
        ReadOperations readOperations = mock( ReadOperations.class );
        Read read = mock( Read.class );
        NodeCursor nodeCursor = mock( NodeCursor.class );
        when( readOperations.nodeGetDegree( 1L, Direction.OUTGOING, 1 ) ).thenReturn( 1 );
        when( readOperations.nodeGetDegree( 2L, Direction.INCOMING, 1 ) ).thenReturn( 3 );

        // WHEN
        connectingRelationships( readOperations, read, mock( CursorFactory.class ), nodeCursor, 1L, Direction.OUTGOING, 2L, new int[]{1} );

        // THEN
        verify( read, times( 1 ) ).singleNode( 1L, nodeCursor);
    }

    @Test
    public void shouldSwitchOrderIfItHasLowerDegreeWithTypes() throws EntityNotFoundException
    {
        // GIVEN
        ReadOperations readOperations = mock( ReadOperations.class );
        Read read = mock( Read.class );
        NodeCursor nodeCursor = mock( NodeCursor.class );
        when( readOperations.nodeGetDegree( 1L, Direction.OUTGOING, 1 ) ).thenReturn( 3 );
        when( readOperations.nodeGetDegree( 2L, Direction.INCOMING, 1 ) ).thenReturn( 1 );

        // WHEN
        connectingRelationships( readOperations, read, mock( CursorFactory.class ), nodeCursor , 1L, Direction.OUTGOING, 2L, new int[]{1} );

        // THEN
        verify( read, times( 1 ) ).singleNode( 2L, nodeCursor );
    }

}
