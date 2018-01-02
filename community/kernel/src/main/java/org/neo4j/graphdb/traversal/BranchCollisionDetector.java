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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;

/**
 * In a bidirectional traversal there's one traversal from each start/end side and
 * they will probably meet somewhere in the middle and the full paths are formed.
 * This is where that detection and path generation takes place.
 * 
 * @author Mattias Persson
 */
public interface BranchCollisionDetector
{
    /**
     * Evaluate the given {@code branch} coming from either the start side or the
     * end side. Which side the branch represents is controlled by the {@code direction}
     * argument, {@link Direction#OUTGOING} means the start side and {@link Direction#INCOMING}
     * means the end side. Returns an {@link Iterable} of new unique {@link Path}s if
     * this branch resulted in a collision with other previously registered branches,
     * or {@code null} if this branch didn't result in any collision.
     * 
     * @param branch the {@link TraversalBranch} to check for collision with other
     * previously registered branches.
     * @param direction {@link Direction#OUTGOING} if this branch represents a branch
     * from the start side of this bidirectional traversal, or {@link Direction#INCOMING}
     * for the end side.
     * @return new paths formed if this branch collided with other branches,
     * or {@code null} if no collision occurred.
     */
    Iterable<Path> evaluate( TraversalBranch branch, Direction direction );
}
