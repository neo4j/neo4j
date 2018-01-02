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
package org.neo4j.graphdb.traversal;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.PathExpander;

/**
 * Selects {@link TraversalBranch}s according to breadth first
 * pattern, the most natural ordering in a breadth first search, see
 * http://en.wikipedia.org/wiki/Breadth-first_search
 */
class PreorderBreadthFirstSelector implements BranchSelector
{
    private final Queue<TraversalBranch> queue = new LinkedList<TraversalBranch>();
    private TraversalBranch current;
    private final PathExpander expander;
    
    public PreorderBreadthFirstSelector( TraversalBranch startSource, PathExpander expander )
    {
        this.current = startSource;
        this.expander = expander;
    }

    public TraversalBranch next( TraversalContext metadata )
    {
        TraversalBranch result = null;
        while ( result == null )
        {
            TraversalBranch next = current.next( expander, metadata );
            if ( next != null )
            {
                queue.add( next );
                result = next;
            }
            else
            {
                current = queue.poll();
                if ( current == null )
                {
                    return null;
                }
            }
        }
        return result;
    }
}
