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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;

/**
 * Pair of {@link TreeState}, ability to make decision about which of the two to read and write respectively,
 * depending on the {@link TreeState#isValid() validity} and {@link TreeState#stableGeneration()} of each.
 */
class TreeStatePair
{

    private TreeStatePair()
    {
    }

    /**
     * Initialize state pages because new pages are expected to be allocated directly after
     * the existing highest allocated page. Otherwise there'd be a hole between meta and root pages
     * until they would have been written, which isn't guaranteed to be handled correctly by the page cache.
     *
     * @param cursor {@link PageCursor} assumed to be opened with write capabilities.
     * @throws IOException on {@link PageCursor} error.
     */
    static void initializeStatePages( PageCursor cursor ) throws IOException
    {
        PageCursorUtil.goTo( cursor, "State page A", IdSpace.STATE_PAGE_A );
        PageCursorUtil.goTo( cursor, "State page B", IdSpace.STATE_PAGE_B );
    }

    /**
     * Reads the tree state pair, one from each of {@code pageIdA} and {@code pageIdB}, deciding their validity
     * and returning them as a {@link Pair}.
     * do-shouldRetry is managed inside this method because data is read from two pages.
     *
     * @param cursor {@link PageCursor} to use when reading. This cursor will be moved to the two pages
     * one after the other, to read their states.
     * @param pageIdA page id containing the first state.
     * @param pageIdB page id containing the second state.
     * @return {@link Pair} of both tree states.
     * @throws IOException on {@link PageCursor} reading error.
     */
    static Pair<TreeState,TreeState> readStatePages( PageCursor cursor, long pageIdA, long pageIdB ) throws IOException
    {
        TreeState stateA = readStatePage( cursor, pageIdA );
        TreeState stateB = readStatePage( cursor, pageIdB );
        return Pair.of( stateA, stateB );
    }

    private static TreeState readStatePage( PageCursor cursor, long pageIdA ) throws IOException
    {
        PageCursorUtil.goTo( cursor, "state page", pageIdA );
        TreeState state;
        do
        {
            state = TreeState.read( cursor );
        }
        while ( cursor.shouldRetry() );
        checkOutOfBounds( cursor );
        return state;
    }

    /**
     * @param states the two states to compare.
     * @return newest (w/ regards to {@link TreeState#stableGeneration()}) {@link TreeState#isValid() valid}
     * {@link TreeState} of the two.
     * @throws IllegalStateException if none were valid.
     */
    static TreeState selectNewestValidState( Pair<TreeState,TreeState> states )
    {
        return selectNewestValidStateOptionally( states ).orElseThrow( () ->
                new TreeInconsistencyException( "Unexpected combination of state.%n  STATE_A[%s]%n  STATE_B[%s]",
                        states.getLeft(), states.getRight() ) );
    }

    /**
     * @param states the two states to compare.
     * @return oldest (w/ regards to {@link TreeState#stableGeneration()}) {@link TreeState#isValid() invalid}
     * {@link TreeState} of the two. If both are invalid then the {@link Pair#getLeft() first one} is returned.
     */
    static TreeState selectOldestOrInvalid( Pair<TreeState,TreeState> states )
    {
        TreeState newestValidState = selectNewestValidStateOptionally( states ).orElse( states.getRight() );
        return newestValidState == states.getLeft() ? states.getRight() : states.getLeft();
    }

    private static Optional<TreeState> selectNewestValidStateOptionally( Pair<TreeState,TreeState> states )
    {
        TreeState stateA = states.getLeft();
        TreeState stateB = states.getRight();

        if ( stateA.isValid() != stateB.isValid() )
        {
            // return only valid
            return stateA.isValid() ? Optional.of( stateA ) : Optional.of( stateB );
        }
        else if ( stateA.isValid() && stateB.isValid() )
        {
            // return newest

            // compare unstable generations of A/B, if equal, compare clean flag (clean is newer than dirty)
            // and include sanity check for stable generations such that there cannot be a state S compared
            // to other state O where
            // S.unstableGeneration > O.unstableGeneration AND S.stableGeneration < O.stableGeneration

            if ( stateA.stableGeneration() == stateB.stableGeneration() &&
                    stateA.unstableGeneration() == stateB.unstableGeneration() &&
                    stateA.isClean() != stateB.isClean() )
            {
                return Optional.of( stateA.isClean() ? stateA : stateB );
            }
            else if ( stateA.stableGeneration() >= stateB.stableGeneration() &&
                    stateA.unstableGeneration() > stateB.unstableGeneration() )
            {
                return Optional.of( stateA );
            }
            else if ( stateA.stableGeneration() <= stateB.stableGeneration() &&
                    stateA.unstableGeneration() < stateB.unstableGeneration() )
            {
                return Optional.of( stateB );
            }
        }

        // return null communicating that this combination didn't result in any valid "newest" state
        return Optional.empty();
    }
}
