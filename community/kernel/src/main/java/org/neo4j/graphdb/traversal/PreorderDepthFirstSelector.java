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

import org.neo4j.graphdb.PathExpander;

/**
 * Selects {@link TraversalBranch}s according to preorder depth first pattern,
 * the most natural ordering in a depth first search, see
 * http://en.wikipedia.org/wiki/Depth-first_search
 */
class PreorderDepthFirstSelector implements BranchSelector
{
    private TraversalBranch current;
    private final PathExpander expander;
    
    PreorderDepthFirstSelector( TraversalBranch startSource, PathExpander expander )
    {
        this.current = startSource;
        this.expander = expander;
    }
    
    public TraversalBranch next( TraversalContext metadata )
    {
        TraversalBranch result = null;
        while ( result == null )
        {
            if ( current == null )
            {
                return null;
            }
            TraversalBranch next = current.next( expander, metadata );
            if ( next == null )
            {
                current = current.parent();
                continue;
            }
            else
            {
                current = next;
            }
            if ( current != null )
            {
                result = current;
            }
        }
        return result;
    }
}
