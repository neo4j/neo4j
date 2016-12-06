/*
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
package org.neo4j.index.gbptree;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class TreeStatePairTest
{
    @Parameterized.Parameters( name = "{0},{1}" )
    public static Collection<Object[]> variants()
    {
        // State A, State B, Select newest, Select oldest

        Collection<Object[]> variants = new ArrayList<>();
        variants.add( new Object[] {State.EMPTY, State.EMPTY, Selected.FAIL, Selected.A} );
        variants.add( new Object[] {State.EMPTY, State.BROKEN, Selected.FAIL, Selected.A} );
        variants.add( new Object[] {State.EMPTY, State.VALID, Selected.B, Selected.A} );

        variants.add( new Object[] {State.BROKEN, State.EMPTY, Selected.FAIL, Selected.A} );
        variants.add( new Object[] {State.BROKEN, State.BROKEN, Selected.FAIL, Selected.A} );
        variants.add( new Object[] {State.BROKEN, State.VALID, Selected.B, Selected.A} );

        variants.add( new Object[] {State.VALID, State.EMPTY, Selected.A, Selected.B} );
        variants.add( new Object[] {State.VALID, State.BROKEN, Selected.A, Selected.B} );
        variants.add( new Object[] {State.VALID, State.OLD_VALID, Selected.A, Selected.B} );
        variants.add( new Object[] {State.VALID, State.VALID, Selected.FAIL, Selected.A} );
        variants.add( new Object[] {State.OLD_VALID, State.VALID, Selected.B, Selected.A} );
        return variants;
    }

    private static final long PAGE_A = 1;
    private static final long PAGE_B = 2;

    @Parameterized.Parameter( 0 )
    public State stateA;
    @Parameterized.Parameter( 1 )
    public State stateB;
    @Parameterized.Parameter( 2 )
    public Selected expectedNewest;
    @Parameterized.Parameter( 3 )
    public Selected expectedOldest;

    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( 256 );

    @Test
    public void shouldCorrectSelectNewestAndOldestState() throws Exception
    {
        // GIVEN
        cursor.next( PAGE_A );
        stateA.write( cursor );
        cursor.next( PAGE_B );
        stateB.write( cursor );

        // WHEN
        Pair<TreeState,TreeState> states = TreeStatePair.readStatePages( cursor, PAGE_A, PAGE_B );

        // THEN
        expectedNewest.verify( states, SelectionUseCase.NEWEST );
        expectedOldest.verify( states, SelectionUseCase.OLDEST );
    }

    enum SelectionUseCase
    {
        NEWEST
        {
            @Override
            TreeState select( Pair<TreeState,TreeState> states )
            {
                return TreeStatePair.selectNewestValidState( states );
            }
        },
        OLDEST
        {
            @Override
            TreeState select( Pair<TreeState,TreeState> states )
            {
                return TreeStatePair.selectOldestOrInvalid( states );
            }
        };

        abstract TreeState select( Pair<TreeState,TreeState> states );
    }

    enum State
    {
        EMPTY
        {
            @Override
            void write( PageCursor cursor )
            {
                // Nothing to write
            }
        },
        BROKEN
        {
            @Override
            void write( PageCursor cursor ) throws IOException
            {
                TreeState.write( cursor, 1, 2, 3, 4 );
                cursor.rewind();
                // flip some of the bits as to break the checksum
                long someOfTheBits = cursor.getLong( cursor.getOffset() );
                cursor.putLong( cursor.getOffset(), ~someOfTheBits );
            }
        },
        VALID
        {
            @Override
            void write( PageCursor cursor ) throws IOException
            {
                TreeState.write( cursor, 5, 6, 7, 8 );
            }
        },
        OLD_VALID
        {
            @Override
            void write( PageCursor cursor ) throws IOException
            {
                TreeState.write( cursor, 2, 3, 4, 5 );
            }
        };

        abstract void write( PageCursor cursor ) throws IOException;
    }

    enum Selected
    {
        FAIL
        {
            @Override
            void verify( Pair<TreeState,TreeState> states, SelectionUseCase selection )
            {
                try
                {
                    selection.select( states );
                    fail( "Should have thrown" );
                }
                catch ( TreeInconsistencyException e )
                {
                    // good
                }
            }
        },
        A
        {
            @Override
            void verify( Pair<TreeState,TreeState> states, SelectionUseCase selection )
            {
                assertSame( states.getLeft(), selection.select( states ) );
            }
        },
        B
        {
            @Override
            void verify( Pair<TreeState,TreeState> states, SelectionUseCase selection )
            {
                assertSame( states.getRight(), selection.select( states ) );
            }
        };

        abstract void verify( Pair<TreeState,TreeState> states, SelectionUseCase selection );
    }
}
