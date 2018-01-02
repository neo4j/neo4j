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
package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

public class AlternatingSelectorOrderer extends AbstractSelectorOrderer<Integer>
{
    public AlternatingSelectorOrderer( BranchSelector startSelector, BranchSelector endSelector )
    {
        super( startSelector, endSelector );
    }
    
    @Override
    protected Integer initialState()
    {
        return 0;
    }

    @Override
    public TraversalBranch next( TraversalContext metadata )
    {
        TraversalBranch branch = nextBranchFromNextSelector( metadata, true );
        Integer previousDepth = getStateForCurrentSelector();
        if ( branch != null && branch.length() == previousDepth.intValue() )
        {
            return branch;
        }
        else
        {
            if ( branch != null )
                setStateForCurrentSelector( branch.length() );
        }
        return branch;
    }
}
