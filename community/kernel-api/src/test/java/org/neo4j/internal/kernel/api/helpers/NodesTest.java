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
package org.neo4j.internal.kernel.api.helpers;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countAll;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countIncoming;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countOutgoing;

public class NodesTest
{

    @Test
    public void shouldCountOutgoingDense()
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
        int count = countOutgoing( new StubNodeCursor( true ), cursors );

        // Then
        assertThat( count, equalTo( 24 ) );
    }

    @Test
    public void shouldCountOutgoingSparse()
    {
        // Given
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain( 11 )
                        .outgoing( 55, 0, 1 )
                        .incoming( 56, 0, 1 )
                        .outgoing( 57, 0, 1 )
                        .loop( 58, 0 ) );
        StubCursorFactory cursors = new StubCursorFactory().withRelationshipTraversalCursors( relationshipCursor );

        // When
        StubNodeCursor nodeCursor = new StubNodeCursor( false ).withNode( 11 );
        nodeCursor.next();
        int count = countOutgoing( nodeCursor, cursors );

        // Then
        assertThat( count, equalTo( 3 ) );
    }

    @Test
    public void shouldCountIncomingDense()
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
        int count = countIncoming( new StubNodeCursor( true ), cursors );

        // Then
        assertThat( count, equalTo( 17 ) );
    }

    @Test
    public void shouldCountIncomingSparse()
    {
        // Given
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain( 11 )
                        .outgoing( 55, 0, 1 )
                        .incoming( 56, 0, 1 )
                        .outgoing( 57, 0, 1 )
                        .loop( 58, 0 ) );
        StubCursorFactory cursors = new StubCursorFactory().withRelationshipTraversalCursors( relationshipCursor );

        StubNodeCursor nodeCursor = new StubNodeCursor( false ).withNode( 11 );
        nodeCursor.next();

        // When
        int count = countIncoming( nodeCursor, cursors );

        // Then
        assertThat( count, equalTo( 2 ) );
    }

    @Test
    public void shouldCountAllDense()
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
        int count = countAll( new StubNodeCursor( true ), cursors );

        // Then
        assertThat( count, equalTo( 29 ) );
    }

    @Test
    public void shouldCountAllSparse()
    {
        // Given
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain( 11 )
                        .outgoing( 55, 0, 1 )
                        .incoming( 56, 0, 1 )
                        .outgoing( 57, 0, 1 )
                        .loop( 58, 0 ) );
        StubCursorFactory cursors = new StubCursorFactory().withRelationshipTraversalCursors( relationshipCursor );

        StubNodeCursor nodeCursor = new StubNodeCursor( false ).withNode( 11 );
        nodeCursor.next();

        // When
        int count = countAll( nodeCursor, cursors );

        // Then
        assertThat( count, equalTo( 4 ) );
    }

    @Test
    public void shouldCountOutgoingDenseWithType()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group( 1 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group( 2 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor, groupCursor );

        // Then
        assertThat( countOutgoing( new StubNodeCursor( true ), cursors, 1 ), equalTo( 6 ) );
        assertThat( countOutgoing( new StubNodeCursor( true ), cursors, 2 ), equalTo( 4 ) );
    }

    @Test
    public void shouldCountOutgoingSparseWithType()
    {
        // Given
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain( 11 )
                        .outgoing( 55, 0, 1 )
                        .incoming( 56, 0, 1 )
                        .outgoing( 57, 0, 1 )
                        .loop( 58, 2 ) );
        StubCursorFactory cursors = new StubCursorFactory( true )
                .withRelationshipTraversalCursors( relationshipCursor );

        // Then
        StubNodeCursor nodeCursor = new StubNodeCursor( false ).withNode( 11 );
        nodeCursor.next();
        assertThat( countOutgoing( nodeCursor, cursors, 1 ), equalTo( 2 ) );
        nodeCursor = new StubNodeCursor( false ).withNode( 11 );
        nodeCursor.next();
        assertThat( countOutgoing( nodeCursor, cursors, 2 ), equalTo( 1 ) );
    }

    @Test
    public void shouldCountIncomingWithTypeDense()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group( 1 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group( 2 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor, groupCursor );

        // Then
        assertThat( countIncoming( new StubNodeCursor( true ), cursors, 1 ), equalTo( 6 ) );
        assertThat( countIncoming( new StubNodeCursor( true ), cursors, 2 ), equalTo( 4 ) );
    }
    @Test
    public void shouldCountIncomingWithTypeSparse()
    {
        // Given
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain( 11 )
                        .outgoing( 55, 0, 1 )
                        .incoming( 56, 0, 1 )
                        .outgoing( 57, 0, 1 )
                        .loop( 58, 2 ) );
        StubCursorFactory cursors = new StubCursorFactory( true )
                .withRelationshipTraversalCursors( relationshipCursor );

        // Then
        StubNodeCursor nodeCursor = new StubNodeCursor( false ).withNode( 11 );
        nodeCursor.next();
        assertThat( countIncoming( nodeCursor, cursors, 1 ), equalTo( 1 ) );
        nodeCursor = new StubNodeCursor( false ).withNode( 11 );
        nodeCursor.next();
        assertThat( countIncoming( nodeCursor, cursors, 2 ), equalTo( 1 ) );
    }

    @Test
    public void shouldCountAllWithTypeDense()
    {
        // Given
        StubGroupCursor groupCursor = new StubGroupCursor(
                group( 1 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 5 ),
                group( 2 ).withOutCount( 1 ).withInCount( 1 ).withLoopCount( 3 )
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors( groupCursor, groupCursor );

        // Then
        assertThat( countAll( new StubNodeCursor( true ), cursors, 1 ), equalTo( 7 ) );
        assertThat( countAll( new StubNodeCursor( true ), cursors, 2 ), equalTo( 5 ) );
    }

    @Test
    public void shouldCountAllWithTypeSparse()
    {
        // Given
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain( 11 )
                        .outgoing( 55, 0, 1 )
                        .incoming( 56, 0, 1 )
                        .outgoing( 57, 0, 1 )
                        .loop( 58, 2 ) );
        StubCursorFactory cursors = new StubCursorFactory( true )
                .withRelationshipTraversalCursors( relationshipCursor );

        // Then
        StubNodeCursor nodeCursor = new StubNodeCursor( false ).withNode( 11 );
        nodeCursor.next();
        assertThat( countAll( nodeCursor, cursors, 1 ), equalTo( 3 ) );
        assertThat( countAll( nodeCursor, cursors, 2 ), equalTo( 1) );
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
