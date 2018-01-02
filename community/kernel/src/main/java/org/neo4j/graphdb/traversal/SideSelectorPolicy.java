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

/**
 * A factory for {@link SideSelector}s. Used during bidirectional traversals.
 * 
 * @author Mattias Persson
 */
public interface SideSelectorPolicy
{
    /**
     * Creates a new {@link SideSelector} given the {@code start}/{@code end}
     * {@link BranchSelector}s and an optional {@code maxDepth}.
     * 
     * @param start the start side {@link BranchSelector} of this
     * bidirectional traversal.
     * @param end the end side {@link BranchSelector} of this
     * bidirectional traversal.
     * @param maxDepth an optional max depth the combined traversal depth must
     * be kept within. Optional in the sense that only some implementations
     * honors it.
     * @return a new {@link SideSelector} for {@code start} and {@code end}
     * {@link BranchSelector}s.
     */
    SideSelector create( BranchSelector start, BranchSelector end, int maxDepth );
}
