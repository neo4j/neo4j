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
package org.neo4j.internal.kernel.api.helpers;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countAll;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countIncoming;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countOutgoing;

public class NodesTest
{
    private static final StubNodeCursor NODE = new StubNodeCursor();

    @Test
    public void shouldCountOutgoing()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group().withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group().withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 ),
                group().withOutCount( 2 ).withInCount( 1 ).withLoopCount( 2 ),
                group().withOutCount( 3 ).withInCount( 1 ).withLoopCount( 1 ),
                group().withOutCount( 5 ).withInCount( 1 ).withLoopCount( 1 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor );

        // When
        int count = countOutgoing( NODE, cursors );

        // Then
        assertThat( count, equalTo( 24 ) );
    }

    @Test
    public void shouldCountIncoming()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group().withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group().withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 ),
                group().withOutCount( 2 ).withInCount( 1 ).withLoopCount( 2 ),
                group().withOutCount( 3 ).withInCount( 1 ).withLoopCount( 1 ),
                group().withOutCount( 5 ).withInCount( 1 ).withLoopCount( 1 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor );

        // When
        int count = countIncoming( new StubNodeCursor(), cursors );

        // Then
        assertThat( count, equalTo( 17 ) );
    }

    @Test
    public void shouldCountAll()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group().withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group().withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 ),
                group().withOutCount( 2 ).withInCount( 1 ).withLoopCount( 2 ),
                group().withOutCount( 3 ).withInCount( 1 ).withLoopCount( 1 ),
                group().withOutCount( 5 ).withInCount( 1 ).withLoopCount( 1 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor );

        // When
        int count = countAll( new StubNodeCursor(), cursors );

        // Then
        assertThat( count, equalTo( 29 ) );
    }

    @Test
    public void shouldCountOutgoingWithType()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group( 1 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group( 2 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor, groupCursor );

        // Then
        assertThat( countOutgoing( new StubNodeCursor(), cursors, 1 ), equalTo( 6 ) );
        assertThat( countOutgoing( new StubNodeCursor(), cursors, 2 ), equalTo( 4 ) );
    }

    @Test
    public void shouldCountIncomingWithType()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group( 1 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group( 2 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor, groupCursor );

        // Then
        assertThat( countIncoming( new StubNodeCursor(), cursors, 1 ), equalTo( 6 ) );
        assertThat( countIncoming( new StubNodeCursor(), cursors, 2 ), equalTo( 4 ) );
    }

    @Test
    public void shouldCountAllWithType()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group( 1 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group( 2 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor, groupCursor );

        // Then
        assertThat( countAll( new StubNodeCursor(), cursors, 1 ), equalTo( 7 ) );
        assertThat( countAll( new StubNodeCursor(), cursors, 2 ), equalTo( 5 ) );
    }

    private StubGroupCursor.GroupData group()
    {
        return new StubGroupCursor.GroupData( 0, 0, 0, 0 );
    }

    private StubGroupCursor.GroupData group( int type )
    {
        return new StubGroupCursor.GroupData( 0, 0, 0, type );
    }
}
