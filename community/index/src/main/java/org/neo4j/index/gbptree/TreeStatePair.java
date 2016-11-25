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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;

class TreeStatePair
{
    static Pair<TreeState,TreeState> readStatePages( PageCursor cursor, long pageIdA, long pageIdB ) throws IOException
    {
        TreeState stateA;
        TreeState stateB;

        // Use write lock to avoid should retry. Fine because there can be no concurrency at this point anyway

        if ( !cursor.next( pageIdA ) )
        {
            throw new IllegalStateException( "Could not open STATE_PAGE_A " + IdSpace.STATE_PAGE_A );
        }
        stateA = TreeState.read( cursor );

        if ( !cursor.next( pageIdB ) )
        {
            throw new IllegalStateException( "Could not open STATE_PAGE_B " + IdSpace.STATE_PAGE_B );
        }
        stateB = TreeState.read( cursor );
        return Pair.of( stateA, stateB );
    }

    static TreeState selectNewestValidState( Pair<TreeState,TreeState> states )
    {
        TreeState selected = selectNewestValidStateOrNull( states );
        if ( selected != null )
        {
            return selected;
        }

        // Fail
        throw new IllegalStateException( format( "Unexpected combination of state.%n  STATE_A=%s%n  STATE_B=%s",
                states.getLeft(), states.getRight() ) );
    }

    static TreeState selectOldestOrInvalid( Pair<TreeState,TreeState> states )
    {
        TreeState newestValidState = selectNewestValidStateOrNull( states );
        if ( newestValidState == null )
        {
            return states.getLeft();
        }
        return newestValidState == states.getLeft() ? states.getRight() : states.getLeft();
    }

    private static TreeState selectNewestValidStateOrNull( Pair<TreeState,TreeState> states )
    {
        TreeState stateA = states.getLeft();
        TreeState stateB = states.getRight();

        if ( stateA.isValid() != stateB.isValid() )
        {
            // return only valid
            return stateA.isValid() ? stateA : stateB;
        }
        else if ( stateA.isValid() && stateB.isValid() )
        {
            // return newest
            if ( stateA.stableGeneration() > stateB.stableGeneration() &&
                    stateA.unstableGeneration() > stateB.unstableGeneration() )
            {
                return stateA;
            }
            else if ( stateA.stableGeneration() < stateB.stableGeneration() &&
                    stateA.unstableGeneration() < stateB.unstableGeneration() )
            {
                return stateB;
            }
        }

        // return null communicating that this combination didn't result in any valid "newest" state
        return null;
    }
}
