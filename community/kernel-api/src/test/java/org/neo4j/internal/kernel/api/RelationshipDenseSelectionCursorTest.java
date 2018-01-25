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

import java.util.ArrayList;
import java.util.List;

public class RelationshipDenseSelectionCursorTest extends RelationshipSelectionTestBase
{

    private TestRelationshipChain outA = new TestRelationshipChain();

    private TestRelationshipChain inA = new TestRelationshipChain()
            .incoming( 0, 1, typeA )
            .incoming( 0, 2, typeA );

    private TestRelationshipChain loopA = new TestRelationshipChain()
            .loop( 0, typeA );

    private TestRelationshipChain outB = new TestRelationshipChain()
            .outgoing( 0, 10, typeB );

    private TestRelationshipChain inB = new TestRelationshipChain();

    private TestRelationshipChain loopB = new TestRelationshipChain()
            .loop( 0, typeB )
            .loop( 0, typeB );

    private TestRelationshipChain outC = new TestRelationshipChain()
            .outgoing( 0, 20, typeC )
            .outgoing( 0, 21, typeC );

    private TestRelationshipChain inC = new TestRelationshipChain()
            .incoming( 0, 22, typeC );

    private TestRelationshipChain loopC = new TestRelationshipChain();

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

    @Test
    public void shouldSelectOutgoing()
    {
        // given
        RelationshipDenseSelectionCursor cursor = new RelationshipDenseSelectionCursor();

        // when
        cursor.outgoing( innerGroupCursor, innerRelationshipCursor );

        // then
        assertLoop( cursor, typeA );
        assertOutgoing( cursor, 10, typeB );
        assertLoop( cursor, typeB );
        assertLoop( cursor, typeB );
        assertOutgoing( cursor, 20, typeC );
        assertOutgoing( cursor, 21, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectIncoming()
    {
        // given
        RelationshipDenseSelectionCursor cursor = new RelationshipDenseSelectionCursor();

        // when
        cursor.incoming( innerGroupCursor, innerRelationshipCursor );

        // then
        assertIncoming( cursor, 1, typeA );
        assertIncoming( cursor, 2, typeA );
        assertLoop( cursor, typeA );
        assertLoop( cursor, typeB );
        assertLoop( cursor, typeB );
        assertIncoming( cursor, 22, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectAll()
    {
        // given
        RelationshipDenseSelectionCursor cursor = new RelationshipDenseSelectionCursor();

        // when
        cursor.all( innerGroupCursor, innerRelationshipCursor );

        // then
        assertIncoming( cursor, 1, typeA );
        assertIncoming( cursor, 2, typeA );
        assertLoop( cursor, typeA );
        assertOutgoing( cursor, 10, typeB );
        assertLoop( cursor, typeB );
        assertLoop( cursor, typeB );
        assertOutgoing( cursor, 20, typeC );
        assertOutgoing( cursor, 21, typeC );
        assertIncoming( cursor, 22, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectOutgoingOfType()
    {
        // given
        RelationshipDenseSelectionCursor cursor = new RelationshipDenseSelectionCursor();

        // when
        cursor.outgoing( innerGroupCursor, innerRelationshipCursor, types( typeA, typeC ) );

        // then
        assertLoop( cursor, typeA );
        assertOutgoing( cursor, 20, typeC );
        assertOutgoing( cursor, 21, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectIncomingOfType()
    {
        // given
        RelationshipDenseSelectionCursor cursor = new RelationshipDenseSelectionCursor();

        // when
        cursor.incoming( innerGroupCursor, innerRelationshipCursor, types( typeA, typeC ) );

        // then
        assertIncoming( cursor, 1, typeA );
        assertIncoming( cursor, 2, typeA );
        assertLoop( cursor, typeA );
        assertIncoming( cursor, 22, typeC );
        assertEmpty( cursor );
    }

    @Test
    public void shouldSelectAllOfType()
    {
        // given
        RelationshipDenseSelectionCursor cursor = new RelationshipDenseSelectionCursor();

        // when
        cursor.all( innerGroupCursor, innerRelationshipCursor, types( typeA, typeC ) );

        // then
        assertIncoming( cursor, 1, typeA );
        assertIncoming( cursor, 2, typeA );
        assertLoop( cursor, typeA );
        assertOutgoing( cursor, 20, typeC );
        assertOutgoing( cursor, 21, typeC );
        assertIncoming( cursor, 22, typeC );
        assertEmpty( cursor );
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
