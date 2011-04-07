/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

/**
 * Common {@link Evaluator}s useful during common traversals.
 * 
 * @author Mattias Persson
 * @see Evaluator
 * @see TraversalDescription
 */
public abstract class Evaluators
{
    private static final Evaluator ALL = new Evaluator()
    {
        public Evaluation evaluate( Path path )
        {
            return Evaluation.INCLUDE_AND_CONTINUE;
        }
    };
    
    private static final Evaluator ALL_BUT_START_POSITION = new Evaluator()
    {
        public Evaluation evaluate( Path path )
        {
            return path.length() == 0 ? Evaluation.EXCLUDE_AND_CONTINUE : Evaluation.INCLUDE_AND_CONTINUE;
        }
    };
    
    /**
     * @return an evaluator which includes everything it encounters and doesn't prune
     * anything.
     */
    public static Evaluator all()
    {
        return ALL;
    }
    
    /**
     * @return an evaluator which never prunes and includes everything except
     * the first position, i.e. the the start node.
     */
    public static Evaluator excludeStartPosition()
    {
        return ALL_BUT_START_POSITION;
    }
    
    /**
     * Returns an {@link Evaluator} which includes positions down to {@code depth}
     * and prunes everything deeper than that.
     * 
     * @param depth the max depth to traverse to.
     * @return Returns an {@link Evaluator} which includes positions down to
     * {@code depth} and prunes everything deeper than that.
     */
    public static Evaluator toDepth( final int depth )
    {
        return new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                return path.length() < depth ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.INCLUDE_AND_PRUNE;
            }
        };
    }
    
    /**
     * Returns an {@link Evaluator} which only includes positions from {@code depth}
     * and deeper and never prunes anything.
     * 
     * @param depth the depth to start include positions from.
     * @return Returns an {@link Evaluator} which only includes positions from
     * {@code depth} and deeper and never prunes anything.
     */
    public static Evaluator fromDepth( final int depth )
    {
        return new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                return path.length() < depth ? Evaluation.EXCLUDE_AND_CONTINUE : Evaluation.INCLUDE_AND_CONTINUE;
            }
        };
    }
    
    /**
     * Returns an {@link Evaluator} which only includes positions at {@code depth}
     * and prunes everything deeper than that.
     * 
     * @param depth the depth to start include positions from.
     * @return Returns an {@link Evaluator} which only includes positions at
     * {@code depth} and prunes everything deeper than that.
     */
    public static Evaluator atDepth( final int depth )
    {
        return new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                return path.length() < depth ? Evaluation.EXCLUDE_AND_CONTINUE : Evaluation.INCLUDE_AND_PRUNE;
            }
        };
    }
    
    /**
     * Returns an {@link Evaluator} which only includes positions between
     * depths {@code minDepth} and {@code maxDepth}. It prunes everything deeper
     * than {@code maxDepth}.
     * 
     * @param minDepth minimum depth a position must have to be included.
     * @param maxDepth maximum depth a position must have to be included.
     * @return Returns an {@link Evaluator} which only includes positions between
     * depths {@code minDepth} and {@code maxDepth}. It prunes everything deeper
     * than {@code maxDepth}.
     */
    public static Evaluator includingDepths( final int minDepth, final int maxDepth )
    {
        return new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                int length = path.length();
                return Evaluation.of( length >= minDepth && length <= maxDepth, length < maxDepth );
            }
        };
    }
}
