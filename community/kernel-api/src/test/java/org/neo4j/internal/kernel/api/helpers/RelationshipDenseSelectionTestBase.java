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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.ResourceIterator;

import static org.junit.Assert.assertTrue;

public abstract class RelationshipDenseSelectionTestBase<Traverser extends RelationshipDenseSelection>
        extends RelationshipSelectionTestBase
{

    private TestRelationshipChain outA = new TestRelationshipChain( 42L );

    private TestRelationshipChain inA = new TestRelationshipChain( 42L )
            .incoming( 0, 1, typeA )
            .incoming( 0, 2, typeA );

    private TestRelationshipChain loopA = new TestRelationshipChain( 42L )
            .loop( 0, typeA );

    private TestRelationshipChain outB = new TestRelationshipChain( 42L )
            .outgoing( 0, 10, typeB );

    private TestRelationshipChain inB = new TestRelationshipChain( 42L );

    private TestRelationshipChain loopB = new TestRelationshipChain( 42L )
            .loop( 0, typeB )
            .loop( 0, typeB );

    private TestRelationshipChain outC = new TestRelationshipChain( 42L )
            .outgoing( 0, 20, typeC )
            .outgoing( 0, 21, typeC );

    private TestRelationshipChain inC = new TestRelationshipChain( 42L )
            .incoming( 0, 22, typeC );

    private TestRelationshipChain loopC = new TestRelationshipChain( 42L );

    private List<TestRelationshipChain> store = new ArrayList<>();

    private StubGroupCursor innerGroupCursor = new StubGroupCursor(
            group( store, typeA, outA, inA, loopA ),
            group( store, typeB, outB, inB, loopB ),
            group( store, typeC, outC, inC, loopC ) );

    private StubRelationshipCursor innerRelationshipCursor = new StubRelationshipCursor( store );

    @Before
    public void rewindCursor()
    {
        innerGroupCursor.rewind();
    }

    protected abstract Traverser make();

    @Test
    public void shouldSelectOutgoing()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.outgoing( innerGroupCursor, innerRelationshipCursor );

        // then
        assertLoop( traverser, typeA );
        assertOutgoing( traverser, 10, typeB );
        assertLoop( traverser, typeB );
        assertLoop( traverser, typeB );
        assertOutgoing( traverser, 20, typeC );
        assertOutgoing( traverser, 21, typeC );
        assertEmptyAndClosed( traverser );
    }

    @Test
    public void shouldSelectIncoming()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.incoming( innerGroupCursor, innerRelationshipCursor );

        // then
        assertIncoming( traverser, 1, typeA );
        assertIncoming( traverser, 2, typeA );
        assertLoop( traverser, typeA );
        assertLoop( traverser, typeB );
        assertLoop( traverser, typeB );
        assertIncoming( traverser, 22, typeC );
        assertEmptyAndClosed( traverser );
    }

    @Test
    public void shouldSelectAll()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.all( innerGroupCursor, innerRelationshipCursor );

        // then
        assertIncoming( traverser, 1, typeA );
        assertIncoming( traverser, 2, typeA );
        assertLoop( traverser, typeA );
        assertOutgoing( traverser, 10, typeB );
        assertLoop( traverser, typeB );
        assertLoop( traverser, typeB );
        assertOutgoing( traverser, 20, typeC );
        assertOutgoing( traverser, 21, typeC );
        assertIncoming( traverser, 22, typeC );
        assertEmptyAndClosed( traverser );
    }

    @Test
    public void shouldSelectOutgoingOfType()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.outgoing( innerGroupCursor, innerRelationshipCursor, types( typeA, typeC ) );

        // then
        assertLoop( traverser, typeA );
        assertOutgoing( traverser, 20, typeC );
        assertOutgoing( traverser, 21, typeC );
        assertEmptyAndClosed( traverser );
    }

    @Test
    public void shouldSelectIncomingOfType()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.incoming( innerGroupCursor, innerRelationshipCursor, types( typeA, typeC ) );

        // then
        assertIncoming( traverser, 1, typeA );
        assertIncoming( traverser, 2, typeA );
        assertLoop( traverser, typeA );
        assertIncoming( traverser, 22, typeC );
        assertEmptyAndClosed( traverser );
    }

    @Test
    public void shouldSelectAllOfType()
    {
        // given
        Traverser traverser = make();

        // when
        traverser.all( innerGroupCursor, innerRelationshipCursor, types( typeA, typeC ) );

        // then
        assertIncoming( traverser, 1, typeA );
        assertIncoming( traverser, 2, typeA );
        assertLoop( traverser, typeA );
        assertOutgoing( traverser, 20, typeC );
        assertOutgoing( traverser, 21, typeC );
        assertIncoming( traverser, 22, typeC );
        assertEmptyAndClosed( traverser );
    }

    abstract void assertOutgoing( Traverser cursor, int targetNode, int type );

    abstract void assertIncoming( Traverser cursor, int sourceNode, int type );

    abstract void assertLoop( Traverser cursor, int type );

    abstract void assertEmpty( Traverser cursor );

    public static class IteratorTest extends RelationshipDenseSelectionTestBase<RelationshipDenseSelectionIterator<R>>
    {
        @Override
        protected RelationshipDenseSelectionIterator<R> make()
        {
            return new RelationshipDenseSelectionIterator<>( R::new );
        }

        @Override
        void assertOutgoing( RelationshipDenseSelectionIterator<R> iterator, int targetNode, int type )
        {
            assertOutgoing( (ResourceIterator<R>) iterator, targetNode, type );
        }

        @Override
        void assertIncoming( RelationshipDenseSelectionIterator<R> iterator, int sourceNode, int type )
        {
            assertIncoming( (ResourceIterator<R>) iterator, sourceNode, type );
        }

        @Override
        void assertLoop( RelationshipDenseSelectionIterator<R> iterator, int type )
        {
            assertLoop( (ResourceIterator<R>) iterator, type );
        }

        @Override
        void assertEmpty( RelationshipDenseSelectionIterator<R> iterator )
        {
            assertEmpty( (ResourceIterator<R>) iterator );
        }
    }

    public static class CursorTest extends RelationshipDenseSelectionTestBase<RelationshipDenseSelectionCursor>
    {
        @Override
        protected RelationshipDenseSelectionCursor make()
        {
            return new RelationshipDenseSelectionCursor();
        }

        @Override
        void assertOutgoing( RelationshipDenseSelectionCursor iterator, int targetNode, int type )
        {
            assertOutgoing( (RelationshipSelectionCursor) iterator, targetNode, type );
        }

        @Override
        void assertIncoming( RelationshipDenseSelectionCursor iterator, int sourceNode, int type )
        {
            assertIncoming( (RelationshipSelectionCursor) iterator, sourceNode, type );
        }

        @Override
        void assertLoop( RelationshipDenseSelectionCursor iterator, int type )
        {
            assertLoop( (RelationshipSelectionCursor) iterator, type );
        }

        @Override
        void assertEmpty( RelationshipDenseSelectionCursor iterator )
        {
            assertEmpty( (RelationshipSelectionCursor) iterator );
        }
    }

    private void assertEmptyAndClosed( Traverser traverser )
    {
        assertEmpty( traverser );
        assertTrue( "close group cursor", innerGroupCursor.isClosed() );
        assertTrue( "close traversal cursor", innerRelationshipCursor.isClosed() );
    }

    private StubGroupCursor.GroupData group(
            List<TestRelationshipChain> store,
            int type,
            TestRelationshipChain out,
            TestRelationshipChain in,
            TestRelationshipChain loop )
    {
        return new StubGroupCursor.GroupData(
                addToStore( store, out ),
                addToStore( store, in ),
                addToStore( store, loop ),
                type );
    }

    private int addToStore( List<TestRelationshipChain> store, TestRelationshipChain chain )
    {
        int ref = store.size();
        store.add( chain );
        return ref;
    }
}
