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
package org.neo4j.graphdb.traversal;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.impl.traversal.AbstractSelectorOrderer;

public class LevelSelectorOrderer extends AbstractSelectorOrderer<LevelSelectorOrderer.Entry>
{
    private final boolean stopDescentOnResult;
    private final TotalDepth totalDepth = new TotalDepth();
    private final int maxDepth;

    public LevelSelectorOrderer( BranchSelector startSelector, BranchSelector endSelector,
            boolean stopDescentOnResult, int maxDepth )
    {
        super( startSelector, endSelector );
        this.stopDescentOnResult = stopDescentOnResult;
        this.maxDepth = maxDepth;
    }

    @Override
    protected Entry initialState()
    {
        return new Entry();
    }

    @Override
    public TraversalBranch next( TraversalContext metadata )
    {
        TraversalBranch branch = nextBranchFromCurrentSelector( metadata, false );
        Entry state = getStateForCurrentSelector();
        AtomicInteger previousDepth = state.depth;
        if ( branch != null && branch.length() == previousDepth.get() )
        {   // Same depth as previous branch returned from this side.
            return branch;
        }

        if ( branch != null )
        {
            totalDepth.set( currentSide(), branch.length() );
        }
        if ( (stopDescentOnResult && (metadata.getNumberOfPathsReturned() > 0)) ||
                (totalDepth.get() > (maxDepth + 1)) )
        {
            nextSelector();
            return null;
        }

        if ( branch != null )
        {
            previousDepth.set( branch.length() );
            state.branch = branch;
        }
        BranchSelector otherSelector = nextSelector();
        Entry otherState = getStateForCurrentSelector();
        TraversalBranch otherBranch = otherState.branch;
        if ( otherBranch != null )
        {
            otherState.branch = null;
            return otherBranch;
        }

        otherBranch = otherSelector.next( metadata );
        if ( otherBranch != null )
        {
            return otherBranch;
        }
        else
        {
            return branch;
        }
    }

    static class Entry
    {
        private final AtomicInteger depth = new AtomicInteger();
        private TraversalBranch branch;
    }

    private static class TotalDepth
    {
        private int out;
        private int in;

        void set( Direction side, int depth )
        {
            switch ( side )
            {
            case OUTGOING:
                out = depth;
                break;
            case INCOMING:
                in = depth;
                break;
            default:
                break;
            }
        }

        int get()
        {
            return out + in;
        }
    }
}
