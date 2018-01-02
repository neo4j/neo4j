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
package org.neo4j.graphalgo;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalMetadata;

/**
 * Interface of algorithms that finds paths in between two nodes.
 * 
 * @author Tobias Ivarsson
 *
 * @param <P> the path type that the algorithm produces
 */
public interface PathFinder<P extends Path>
{
    /**
     * Tries to find a single path between {@code start} and {@code end}
     * nodes. If a path is found a {@link Path} is returned with that path
     * information, else {@code null} is returned. If more than one path is
     * found, the implementation can decide itself upon which of those to return.
     *
     * @param start the start {@link Node} which defines the start of the path.
     * @param end the end {@link Node} which defines the end of the path.
     * @return a single {@link Path} between {@code start} and {@code end},
     * or {@code null} if no path was found.
     */
    P findSinglePath( Node start, Node end );

    /**
     * Tries to find all paths between {@code start} and {@code end} nodes.
     * A collection of {@link Path}s is returned with all the found paths.
     * If no paths are found an empty collection is returned.
     *
     * @param start the start {@link Node} which defines the start of the path.
     * @param end the end {@link Node} which defines the end of the path.
     * @return all {@link Path}s between {@code start} and {@code end}.
     */
    Iterable<P> findAllPaths( Node start, Node end );
    
    TraversalMetadata metadata();
}
