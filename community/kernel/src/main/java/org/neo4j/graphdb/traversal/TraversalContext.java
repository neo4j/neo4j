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
 * Provides a context for {@link TraversalBranch}es which they need to
 * move further and report their progress.
 * 
 * @author Mattias Persson
 */
public interface TraversalContext extends TraversalMetadata
{
    /**
     * Reports that one more relationship has been traversed in this
     * traversal.
     */
    void relationshipTraversed();
    
    /**
     * Reports that one more relationship has been traversed, albeit
     * a relationship that hasn't provided any benefit to the traversal.
     */
    void unnecessaryRelationshipTraversed();
    
    /**
     * Used for start branches to check adherence to the traversal uniqueness.
     * 
     * @param branch the {@link TraversalBranch} to check for uniqueness.
     * @return {@code true} if the branch is considered unique and is
     * allowed to progress in this traversal.
     */
    boolean isUniqueFirst( TraversalBranch branch );
    
    /**
     * Used for all except branches to check adherence to the traversal
     * uniqueness.
     * 
     * @param branch the {@link TraversalBranch} to check for uniqueness.
     * @return {@code true} if the branch is considered unique and is
     * allowed to progress in this traversal.
     */
    boolean isUnique( TraversalBranch branch );
    
    /**
     * Evaluates a {@link TraversalBranch} whether or not to include it in the
     * result and whether or not to continue further down this branch or not.
     * 
     * @param branch the {@link TraversalBranch} to evaluate.
     * @param state the {@link BranchState} for the branch.
     * @return an {@link Evaluation} of the branch in this traversal.
     */
    @SuppressWarnings( "rawtypes" )
    Evaluation evaluate( TraversalBranch branch, BranchState state );
}
