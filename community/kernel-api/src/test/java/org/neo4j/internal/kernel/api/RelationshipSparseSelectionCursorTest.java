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

import static junit.framework.TestCase.assertTrue;

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
        innerByDir.rewind();
        innerByGroup.rewind();
    }

    @Test
    public void shouldSelectOutgoing()
    {
        // given
        RelationshipSparseSelectionIterator<R> iterator = new RelationshipSparseSelectionIterator<>( R::new );

        // when
        iterator.outgoing( innerByGroup );

        // then
        assertOutgoing( iterator, 10, typeA );
        assertLoop( iterator, typeA );
        assertOutgoing( iterator, 20, typeB );
        assertLoop( iterator, typeB );
        assertOutgoing( iterator, 30, typeC );
        assertLoop( iterator, typeC );
        assertEmptyAndClosed( iterator, innerByGroup );
    }

    @Test
    public void shouldSelectIncoming()
    {
        // given
        RelationshipSparseSelectionIterator<R> iterator = new RelationshipSparseSelectionIterator<>( R::new );

        // when
        iterator.incoming( innerByGroup );

        // then
        assertIncoming( iterator, 11, typeA );
        assertLoop( iterator, typeA );
        assertIncoming( iterator, 21, typeB );
        assertLoop( iterator, typeB );
        assertIncoming( iterator, 31, typeC );
        assertLoop( iterator, typeC );
        assertEmptyAndClosed( iterator, innerByGroup );
    }

    @Test
    public void shouldSelectAll()
    {
        // given
        RelationshipSparseSelectionIterator<R> iterator = new RelationshipSparseSelectionIterator<>( R::new );

        // when
        iterator.all( innerByGroup );

        // then
        assertOutgoing( iterator, 10, typeA );
        assertIncoming( iterator, 11, typeA );
        assertLoop( iterator, typeA );
        assertOutgoing( iterator, 20, typeB );
        assertIncoming( iterator, 21, typeB );
        assertLoop( iterator, typeB );
        assertOutgoing( iterator, 30, typeC );
        assertIncoming( iterator, 31, typeC );
        assertLoop( iterator, typeC );
        assertEmptyAndClosed( iterator, innerByGroup );
    }

    @Test
    public void shouldSelectOutgoingOfType()
    {
        // given
        RelationshipSparseSelectionIterator<R> iterator = new RelationshipSparseSelectionIterator<>( R::new );

        // when
        iterator.outgoing( innerByDir, types( typeA, typeC ) );

        // then
        assertOutgoing( iterator, 10, typeA );
        assertOutgoing( iterator, 12, typeC );
        assertLoop( iterator, typeA );
        assertLoop( iterator, typeC );
        assertEmptyAndClosed( iterator, innerByDir );
    }

    @Test
    public void shouldSelectIncomingOfType()
    {
        // given
        RelationshipSparseSelectionIterator<R> iterator = new RelationshipSparseSelectionIterator<>( R::new );

        // when
        iterator.incoming( innerByDir, types( typeA, typeC ) );

        // then
        assertIncoming( iterator, 20, typeA );
        assertIncoming( iterator, 22, typeC );
        assertLoop( iterator, typeA );
        assertLoop( iterator, typeC );
        assertEmptyAndClosed( iterator, innerByDir );
    }

    @Test
    public void shouldSelectAllOfType()
    {
        // given
        RelationshipSparseSelectionIterator<R> iterator = new RelationshipSparseSelectionIterator<>( R::new );

        // when
        iterator.all( innerByDir, types( typeA, typeC ) );

        // then
        assertOutgoing( iterator, 10, typeA );
        assertOutgoing( iterator, 12, typeC );
        assertIncoming( iterator, 20, typeA );
        assertIncoming( iterator, 22, typeC );
        assertLoop( iterator, typeA );
        assertLoop( iterator, typeC );
        assertEmptyAndClosed( iterator, innerByDir );
    }

    private void assertEmptyAndClosed( RelationshipSparseSelectionIterator<R> iterator, RelationshipTraversalCursor inner )
    {
        assertEmpty( iterator );
        assertTrue( "closed traversal cursor", inner.isClosed() );
    }
}
