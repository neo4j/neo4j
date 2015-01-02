/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
 * Creator of {@link BranchSelector} instances with a starting point to base
 * the first decision on.
 */
public interface BranchOrderingPolicy
{
    /**
     * Instantiates a {@link BranchSelector} with {@code startBranch} as the
     * first branch to base a decision on "where to go next".
     *
     * @param startBranch the {@link TraversalBranch} to start from.
     * @param expander {@link PathExpander} to use for expanding the branch.
     * @return a new {@link BranchSelector} used to decide "where to go next" in
     *         the traversal.
     */
    BranchSelector create( TraversalBranch startBranch, PathExpander expander );
}
