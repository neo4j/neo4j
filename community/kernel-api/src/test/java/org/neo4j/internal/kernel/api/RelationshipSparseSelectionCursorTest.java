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
package org.neo4j.internal.kernel.api;

import org.junit.Before;
import org.junit.Test;

public class RelationshipSparseSelectionCursorTest extends RelationshipSelectionTestBase
{
    private StubRelationshipCursor innerByGroup =
            new StubRelationshipCursor(
                    new TestRelationshipChain()
                            .outgoing( 0, 10, typeA )
                            .incoming( 1, 11, typeA )
                            .loop( 2, typeA )
                            .outgoing( 3, 20, typeB )
                            .incoming( 4, 21, typeB )
                            .loop( 5, typeB )
                            .outgoing( 6, 30, typeC )
                            .incoming( 7, 31, typeC )
                            .loop( 8, typeC ) );

    private StubRelationshipCursor innerByDir =
            new StubRelationshipCursor(
                    new TestRelationshipChain()
                            .outgoing( 1, 10, typeA )
                            .outgoing( 2, 11, typeB )
                            .outgoing( 3, 12, typeC )
                            .incoming( 4, 20, typeA )
                            .incoming( 5, 21, typeB )
                            .incoming( 6, 22, typeC )
                            .loop( 7, typeA )
                            .loop( 8, typeB )
                            .loop( 9, typeC ) );

    @Before
    public void rewindInner()
    {
        innerByGroup.rewind();
    }

    @Test
    public void shouldSelectOutgoing()
    {
        // given
        RelationshipSparseSelectionCursor cursor = new RelationshipSparseSelectionCursor();

        // when
        cursor.outgoing( innerByGroup );

        // then
        assertOutgoing( cursor, 10, typeA );
        assertLoop( cursor, typeA );
        assertOutgoing( cursor, 20, typeB );
        assertLoop( cursor, typeB );
        assertOutgoing( cursor, 30, typeC );
        assertLoop( cursor, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectIncoming()
    {
        // given
        RelationshipSparseSelectionCursor cursor = new RelationshipSparseSelectionCursor();

        // when
        cursor.incoming( innerByGroup );

        // then
        assertIncoming( cursor, 11, typeA );
        assertLoop( cursor, typeA );
        assertIncoming( cursor, 21, typeB );
        assertLoop( cursor, typeB );
        assertIncoming( cursor, 31, typeC );
        assertLoop( cursor, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectAll()
    {
        // given
        RelationshipSparseSelectionCursor cursor = new RelationshipSparseSelectionCursor();

        // when
        cursor.all( innerByGroup );

        // then
        assertOutgoing( cursor, 10, typeA );
        assertIncoming( cursor, 11, typeA );
        assertLoop( cursor, typeA );
        assertOutgoing( cursor, 20, typeB );
        assertIncoming( cursor, 21, typeB );
        assertLoop( cursor, typeB );
        assertOutgoing( cursor, 30, typeC );
        assertIncoming( cursor, 31, typeC );
        assertLoop( cursor, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectOutgoingOfType()
    {
        // given
        RelationshipSparseSelectionCursor cursor = new RelationshipSparseSelectionCursor();

        // when
        cursor.outgoing( innerByDir, types( typeA, typeC ) );

        // then
        assertOutgoing( cursor, 10, typeA );
        assertOutgoing( cursor, 12, typeC );
        assertLoop( cursor, typeA );
        assertLoop( cursor, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectIncomingOfType()
    {
        // given
        RelationshipSparseSelectionCursor cursor = new RelationshipSparseSelectionCursor();

        // when
        cursor.incoming( innerByDir, types( typeA, typeC ) );

        // then
        assertIncoming( cursor, 20, typeA );
        assertIncoming( cursor, 22, typeC );
        assertLoop( cursor, typeA );
        assertLoop( cursor, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectAllOfType()
    {
        // given
        RelationshipSparseSelectionCursor cursor = new RelationshipSparseSelectionCursor();

        // when
        cursor.all( innerByDir, types( typeA, typeC ) );

        // then
        assertOutgoing( cursor, 10, typeA );
        assertOutgoing( cursor, 12, typeC );
        assertIncoming( cursor, 20, typeA );
        assertIncoming( cursor, 22, typeC );
        assertLoop( cursor, typeA );
        assertLoop( cursor, typeC );
        assertEmpty( cursor );
    }
}
