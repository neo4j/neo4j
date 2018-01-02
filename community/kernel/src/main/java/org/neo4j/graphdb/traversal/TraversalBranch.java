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

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;

/**
 * Represents a {@link Path position} and a {@link RelationshipExpander} with a
 * traversal context, for example parent and an iterator of relationships to go
 * next. It's a base to write a {@link BranchSelector} on top of.
 */
public interface TraversalBranch extends Path
{
    /**
     * The parent expansion source which created this {@link TraversalBranch}.
     * @return the parent of this expansion source.
     */
    TraversalBranch parent();

    /**
     * Returns the next expansion source from the expanded relationships
     * from the current node.
     *
     * @param expander an expander to decide which relationships to follow
     * @param metadata the context of the traversal
     * @return the next expansion source from this expansion source.
     */
    TraversalBranch next( PathExpander expander, TraversalContext metadata );

    /**
     * Returns the number of relationships this expansion source has expanded.
     * In this count isn't included the relationship which led to coming here
     * (since that could also be traversed, although skipped, when expanding
     * this source).
     *
     * @return the number of relationships this expansion source has expanded.
     */
    int expanded();
    
    /**
     * Explicitly tell this branch to be pruned so that consecutive calls to
     * {@link #next(PathExpander, TraversalContext)} is guaranteed to return
     * {@code null}.
     */
    void prune();
    
    /**
     * @return whether or not the traversal should continue further along this
     * branch.
     */
    boolean continues();
    
    /**
     * @return whether or not this branch (the {@link Path} representation of
     * this branch at least) should be included in the result of this
     * traversal, i.e. returned as one of the {@link Path}s from f.ex.
     * {@link TraversalDescription#traverse(org.neo4j.graphdb.Node...)}
     */
    boolean includes();
    
    /**
     * Can change evaluation outcome in a negative direction. For example
     * to force pruning.
     * @param eval the {@link Evaluation} to AND with the current evaluation.
     */
    void evaluation( Evaluation eval );
    
    /**
     * Initializes this {@link TraversalBranch}, the relationship iterator,
     * {@link Evaluation} etc.
     * 
     * @param expander {@link RelationshipExpander} to use for getting relationships.
     * @param metadata {@link TraversalContext} to update on progress.
     */
    void initialize( PathExpander expander, TraversalContext metadata );
    
    /**
     * Returns the state associated with this branch.
     * 
     * Why is this of type {@link Object}? The state object type only exists when
     * specifying the expander in the {@link TraversalDescription}, not anywhere
     * else. So in the internals of the traversal the state type is unknown and ignored.
     * 
     * @return the state associated with this branch.
     */
    Object state();
}
