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

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static org.junit.Assert.assertTrue;

public abstract class RelationshipSparseSelectionTestBase<Traverser extends RelationshipSparseSelection>
        extends RelationshipSelectionTestBase
{
    private StubRelationshipCursor innerByGroup =
            new StubRelationshipCursor(
                    new TestRelationshipChain( 42L )
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
                    new TestRelationshipChain( 42L )
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

    protected abstract Traverser make();

    @Test
    public void shouldSelectOutgoing()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.outgoing( innerByGroup );

        // then
        assertOutgoing( traverser, 10, typeA );
        assertLoop( traverser, typeA );
        assertOutgoing( traverser, 20, typeB );
        assertLoop( traverser, typeB );
        assertOutgoing( traverser, 30, typeC );
        assertLoop( traverser, typeC );
        assertEmptyAndClosed( traverser, innerByGroup );
    }

    @Test
    public void shouldSelectIncoming()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.incoming( innerByGroup );

        // then
        assertIncoming( traverser, 11, typeA );
        assertLoop( traverser, typeA );
        assertIncoming( traverser, 21, typeB );
        assertLoop( traverser, typeB );
        assertIncoming( traverser, 31, typeC );
        assertLoop( traverser, typeC );
        assertEmptyAndClosed( traverser, innerByGroup );
    }

    @Test
    public void shouldSelectAll()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.all( innerByGroup );

        // then
        assertOutgoing( traverser, 10, typeA );
        assertIncoming( traverser, 11, typeA );
        assertLoop( traverser, typeA );
        assertOutgoing( traverser, 20, typeB );
        assertIncoming( traverser, 21, typeB );
        assertLoop( traverser, typeB );
        assertOutgoing( traverser, 30, typeC );
        assertIncoming( traverser, 31, typeC );
        assertLoop( traverser, typeC );
        assertEmptyAndClosed( traverser, innerByGroup );
    }

    @Test
    public void shouldSelectOutgoingOfType()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.outgoing( innerByDir, types( typeA, typeC ) );

        // then
        assertOutgoing( traverser, 10, typeA );
        assertOutgoing( traverser, 12, typeC );
        assertLoop( traverser, typeA );
        assertLoop( traverser, typeC );
        assertEmptyAndClosed( traverser, innerByDir );
    }

    @Test
    public void shouldSelectIncomingOfType()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.incoming( innerByDir, types( typeA, typeC ) );

        // then
        assertIncoming( traverser, 20, typeA );
        assertIncoming( traverser, 22, typeC );
        assertLoop( traverser, typeA );
        assertLoop( traverser, typeC );
        assertEmptyAndClosed( traverser, innerByDir );
    }

    @Test
    public void shouldSelectAllOfType()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.all( innerByDir, types( typeA, typeC ) );

        // then
        assertOutgoing( traverser, 10, typeA );
        assertOutgoing( traverser, 12, typeC );
        assertIncoming( traverser, 20, typeA );
        assertIncoming( traverser, 22, typeC );
        assertLoop( traverser, typeA );
        assertLoop( traverser, typeC );
        assertEmptyAndClosed( traverser, innerByDir );
    }

    abstract void assertOutgoing( Traverser cursor, int targetNode, int type );

    abstract void assertIncoming( Traverser cursor, int sourceNode, int type );

    abstract void assertLoop( Traverser cursor, int type );

    abstract void assertEmpty( Traverser cursor );

    public static class IteratorTest extends RelationshipSparseSelectionTestBase<RelationshipSparseSelectionIterator<R>>
    {
        @Override
        protected RelationshipSparseSelectionIterator<R> make()
        {
            return new RelationshipSparseSelectionIterator<>( R::new );
        }

        @Override
        void assertOutgoing( RelationshipSparseSelectionIterator<R> iterator, int targetNode, int type )
        {
            assertOutgoing( (ResourceIterator<R>) iterator, targetNode, type );
        }

        @Override
        void assertIncoming( RelationshipSparseSelectionIterator<R> iterator, int sourceNode, int type )
        {
            assertIncoming( (ResourceIterator<R>) iterator, sourceNode, type );
        }

        @Override
        void assertLoop( RelationshipSparseSelectionIterator<R> iterator, int type )
        {
            assertLoop( (ResourceIterator<R>) iterator, type );
        }

        @Override
        void assertEmpty( RelationshipSparseSelectionIterator<R> iterator )
        {
            assertEmpty( (ResourceIterator<R>) iterator );
        }
    }

    public static class CursorTest extends RelationshipSparseSelectionTestBase<RelationshipSparseSelectionCursor>
    {
        @Override
        protected RelationshipSparseSelectionCursor make()
        {
            return new RelationshipSparseSelectionCursor();
        }

        @Override
        void assertOutgoing( RelationshipSparseSelectionCursor iterator, int targetNode, int type )
        {
            assertOutgoing( (RelationshipSelectionCursor) iterator, targetNode, type );
        }

        @Override
        void assertIncoming( RelationshipSparseSelectionCursor iterator, int sourceNode, int type )
        {
            assertIncoming( (RelationshipSelectionCursor) iterator, sourceNode, type );
        }

        @Override
        void assertLoop( RelationshipSparseSelectionCursor iterator, int type )
        {
            assertLoop( (RelationshipSelectionCursor) iterator, type );
        }

        @Override
        void assertEmpty( RelationshipSparseSelectionCursor iterator )
        {
            assertEmpty( (RelationshipSelectionCursor) iterator );
        }
    }

    private void assertEmptyAndClosed( Traverser traverser, RelationshipTraversalCursor inner )
    {
        assertEmpty( traverser );
        assertTrue( "closed traversal cursor", inner.isClosed() );
    }
}
