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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.PathExpander;

/**
 * Selects {@link TraversalBranch}s according to postorder breadth first
 * pattern which basically is a reverse to preorder breadth first in that
 * deepest levels are returned first, see
 * http://en.wikipedia.org/wiki/Breadth-first_search
 */
class PostorderBreadthFirstSelector implements BranchSelector
{
    private Iterator<TraversalBranch> sourceIterator;
    private TraversalBranch current;
    private final PathExpander expander;
    
    PostorderBreadthFirstSelector( TraversalBranch startSource, PathExpander expander )
    {
        this.current = startSource;
        this.expander = expander;
    }

    public TraversalBranch next( TraversalContext metadata )
    {
        if ( sourceIterator == null )
        {
            sourceIterator = gatherSourceIterator( metadata );
        }
        return sourceIterator.hasNext() ? sourceIterator.next() : null;
    }

    private Iterator<TraversalBranch> gatherSourceIterator( TraversalContext metadata )
    {
        LinkedList<TraversalBranch> queue = new LinkedList<TraversalBranch>();
        queue.add( current.next( expander, metadata ) );
        while ( true )
        {
            List<TraversalBranch> level = gatherOneLevel( queue, metadata );
            if ( level.isEmpty() )
            {
                break;
            }
            queue.addAll( 0, level );
        }
        return queue.iterator();
    }

    private List<TraversalBranch> gatherOneLevel(
            List<TraversalBranch> queue, TraversalContext metadata )
    {
        List<TraversalBranch> level = new LinkedList<TraversalBranch>();
        Integer depth = null;
        for ( TraversalBranch source : queue )
        {
            if ( depth == null )
            {
                depth = source.length();
            }
            else if ( source.length() != depth )
            {
                break;
            }
            
            while ( true )
            {
                TraversalBranch next = source.next( expander, metadata );
                if ( next == null )
                {
                    break;
                }
                level.add( next );
            }
        }
        return level;
    }
}
