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
package org.neo4j.graphalgo.impl.util;

import static org.neo4j.kernel.StandardExpander.toPathExpander;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

/**
 * A preorder depth first selector which detects "super nodes", i.e. nodes
 * which has many relationships. It delays traversing those super nodes until
 * after all non-super nodes have been traversed.
 * 
 * @author Mattias Persson
 * @author Tobias Ivarsson
 */
public class LiteDepthFirstSelector implements BranchSelector
{
    private final Queue<TraversalBranch> superNodes = new LinkedList<TraversalBranch>();
    private TraversalBranch current;
    private final int threshold;
    private final PathExpander expander;
    
    public LiteDepthFirstSelector( TraversalBranch startSource, int startThreshold, PathExpander expander )
    {
        this.current = startSource;
        this.threshold = startThreshold;
        this.expander = expander;
    }
    
    public LiteDepthFirstSelector( TraversalBranch startSource, int startThreshold, RelationshipExpander expander )
    {
        this( startSource, startThreshold, toPathExpander( expander ) );
    }
    
    public TraversalBranch next( TraversalContext metadata )
    {
        TraversalBranch result = null;
        while ( result == null )
        {
            if ( current == null )
            {
                current = superNodes.poll();
                if ( current == null )
                {
                    return null;
                }
            }
            else if ( current.expanded() > 0 && current.expanded() % threshold == 0 )
            {
                superNodes.add( current );
                current = current.parent();
                continue;
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
