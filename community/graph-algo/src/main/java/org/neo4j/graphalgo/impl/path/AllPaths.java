/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.graphalgo.impl.path;

import java.util.Iterator;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class AllPaths implements PathFinder<Path>
{
    private final RelationshipExpander expander;
    private final int maxDepth;

    public AllPaths( int maxDepth, RelationshipExpander expander )
    {
        this.maxDepth = maxDepth;
        this.expander = expander;
    }

    public Iterable<Path> findAllPaths( Node start, final Node end )
    {
        Predicate<Path> filter = new Predicate<Path>()
        {
            public boolean accept( Path pos )
            {
                return pos.endNode().equals( end );
            }
        };

        return Traversal.description().expand( expander ).depthFirst().filter( filter ).prune(
                Traversal.pruneAfterDepth( maxDepth ) ).uniqueness( uniqueness() ).traverse( start );
    }

    protected Uniqueness uniqueness()
    {
        return Uniqueness.RELATIONSHIP_PATH;
    }

    public Path findSinglePath( Node start, Node end )
    {
        Iterator<Path> paths = findAllPaths( start, end ).iterator();
        return paths.hasNext() ? paths.next() : null;
    }
}
