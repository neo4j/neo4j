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
 * Outcome of {@link Evaluator#evaluate(org.neo4j.graphdb.Path)}. An evaluation
 * can tell the traversal whether or not to continue down that
 * {@link TraversalBranch} and whether or not to include a
 * {@link TraversalBranch} in the result of a traversal.
 * 
 * @author Mattias Persson
 * @see Evaluator
 */
public enum Evaluation
{
    INCLUDE_AND_CONTINUE( true, true ),
    INCLUDE_AND_PRUNE( true, false ),
    EXCLUDE_AND_CONTINUE( false, true ),
    EXCLUDE_AND_PRUNE( false, false );
    
    private final boolean includes;
    private final boolean continues;

    private Evaluation( boolean includes, boolean continues )
    {
        this.includes = includes;
        this.continues = continues;
    }

    /**
     * @return whether or not the {@link TraversalBranch} this outcome was
     * generated for should be included in the traversal result.
     */
    public boolean includes()
    {
        return this.includes;
    }
    
    /**
     * @return whether or not the traversal should continue down the
     * {@link TraversalBranch} this outcome was generator for. 
     */
    public boolean continues()
    {
        return continues;
    }
    
    /**
     * Returns an {@link Evaluation} for the given {@code includes} and
     * {@code continues}.
     * 
     * @param includes whether or not to include the {@link TraversalBranch}
     * in the traversal result.
     * @param continues whether or not to continue down the
     * {@link TraversalBranch}.
     * @return an {@link Evaluation} representing {@code includes}
     * and {@code continues}.
     */
    public static Evaluation of( boolean includes, boolean continues )
    {
        return includes?(continues?INCLUDE_AND_CONTINUE:INCLUDE_AND_PRUNE):
                        (continues?EXCLUDE_AND_CONTINUE:EXCLUDE_AND_PRUNE);
    }
    
    /**
     * Returns an {@link Evaluation} for the given {@code includes}, meaning
     * whether or not to include a {@link TraversalBranch} in the traversal
     * result or not. The returned evaluation will always return true
     * for {@link Evaluation#continues()}.
     * 
     * @param includes whether or not to include a {@link TraversalBranch}
     * in the traversal result.
     * @return an {@link Evaluation} representing whether or not to include
     * a {@link TraversalBranch} in the traversal result.
     */
    public static Evaluation ofIncludes( boolean includes )
    {
        return includes?INCLUDE_AND_CONTINUE:EXCLUDE_AND_CONTINUE;
    }
    
    /**
     * Returns an {@link Evaluation} for the given {@code continues}, meaning
     * whether or not to continue further down a {@link TraversalBranch} in the
     * traversal. The returned evaluation will always return true for
     * {@link Evaluation#includes()}.
     * 
     * @param continues whether or not to continue further down a
     *            {@link TraversalBranch} in the traversal.
     * @return an {@link Evaluation} representing whether or not to continue
     *         further down a {@link TraversalBranch} in the traversal.
     */
    public static Evaluation ofContinues( boolean continues )
    {
        return continues?INCLUDE_AND_CONTINUE:INCLUDE_AND_PRUNE;
    }
}
