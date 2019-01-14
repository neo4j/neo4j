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
package org.neo4j.graphalgo.impl.path;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.LimitingResourceIterable;

public abstract class TraversalPathFinder implements PathFinder<Path>
{
    private Traverser lastTraverser;

    @Override
    public Path findSinglePath( Node start, Node end )
    {
        return Iterables.firstOrNull( findAllPaths( start, end ) );
    }

    protected Integer maxResultCount()
    {
        return null;
    }

    @Override
    public Iterable<Path> findAllPaths( Node start, Node end )
    {
        lastTraverser = instantiateTraverser( start, end );
        Integer maxResultCount = maxResultCount();
        return maxResultCount != null ? new LimitingResourceIterable<>( lastTraverser, maxResultCount ) : lastTraverser;
    }

    protected abstract Traverser instantiateTraverser( Node start, Node end );

    @Override
    public TraversalMetadata metadata()
    {
        if ( lastTraverser == null )
        {
            throw new IllegalStateException( "No traversal has been made" );
        }
        return lastTraverser.metadata();
    }
}
