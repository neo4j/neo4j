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
package org.neo4j.graphdb.impl.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.SideSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

public abstract class AbstractSelectorOrderer<T> implements SideSelector
{
    private static final BranchSelector EMPTY_SELECTOR = metadata -> null;

    private final BranchSelector[] selectors;
    @SuppressWarnings( "unchecked" )
    private final T[] states = (T[]) new Object[2];
    private int selectorIndex;

    public AbstractSelectorOrderer( BranchSelector startSelector, BranchSelector endSelector )
    {
        selectors = new BranchSelector[] { startSelector, endSelector };
        states[0] = initialState();
        states[1] = initialState();
    }

    protected T initialState()
    {
        return null;
    }

    protected void setStateForCurrentSelector( T state )
    {
        states[selectorIndex] = state;
    }

    protected T getStateForCurrentSelector()
    {
        return states[selectorIndex];
    }

    protected TraversalBranch nextBranchFromCurrentSelector( TraversalContext metadata,
            boolean switchIfExhausted )
    {
        return nextBranchFromSelector( metadata, selectors[selectorIndex], switchIfExhausted );
    }

    protected TraversalBranch nextBranchFromNextSelector( TraversalContext metadata,
            boolean switchIfExhausted )
    {
        return nextBranchFromSelector( metadata, nextSelector(), switchIfExhausted );
    }

    private TraversalBranch nextBranchFromSelector( TraversalContext metadata,
            BranchSelector selector, boolean switchIfExhausted )
    {
        TraversalBranch result = selector.next( metadata );
        if ( result == null )
        {
            selectors[selectorIndex] = EMPTY_SELECTOR;
            if ( switchIfExhausted )
            {
                result = nextSelector().next( metadata );
                if ( result == null )
                {
                    selectors[selectorIndex] = EMPTY_SELECTOR;
                }
            }
        }
        return result;
    }

    protected BranchSelector nextSelector()
    {
        selectorIndex = (selectorIndex + 1) % 2;
        return selectors[selectorIndex];
    }

    @Override
    public Direction currentSide()
    {
        return selectorIndex == 0 ? Direction.OUTGOING : Direction.INCOMING;
    }

}
