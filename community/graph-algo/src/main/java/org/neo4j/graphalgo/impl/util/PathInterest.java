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
package org.neo4j.graphalgo.impl.util;

import java.util.Comparator;

import org.neo4j.function.BiFunction;

/**
 * PathInterest decides if a path is of interest or not in priority based traversal such as
 * {@link org.neo4j.graphalgo.impl.path.Dijkstra} or {@link org.neo4j.graphalgo.impl.path.AStar}.
 * {@link #comparator()} provides a comparator on priority object to be used when ordering paths.
 * {@link #canBeRuledOut(int, Object, Object)}
 * @author Anton Persson
 */
public interface PathInterest<P>
{
    /**
     * @return {@link java.util.Comparator} to use when ordering in priority map
     */
    Comparator<P> comparator();

    /**
     * Decide if a traversal branch with numberOfVisits, pathPriority and oldPriority (based on end node) can be ruled
     * out from further traversal or not.
     * @param numberOfVisits number of times a traversal branch ending on the same node has been traversed from.
     * @param pathPriority priority of traversal branch currently considered.
     * @param oldPriority priority of other traversal branch.
     * @return true if traversal branch can be ruled out from further traversal, false otherwise.
     */
    boolean canBeRuledOut( int numberOfVisits, P pathPriority, P oldPriority );

    /**
     * Decide if a traversal branch that previously has not been ruled out still is interesting. This would typically
     * mean that a certain number of paths are of interest.
     * @param numberOfVisits
     * @return true if traversal branch still is of interest
     */
    boolean stillInteresting( int numberOfVisits );

    /**
     * Should traversal stop when traversed beyond lowest cost?
     * @return true if traversal should stop beyond lowest cost.
     */
    boolean stopAfterLowestCost();

    public abstract class PriorityBasedPathInterest<P> implements PathInterest<P>
    {
        /**
         * @return {@link org.neo4j.function.BiFunction} to be used when deciding if entity can be ruled out or not.
         */
        abstract BiFunction<P,P,Boolean> interestFunction();

        /**
         *
         * @param numberOfVisits number of times a traversal branch ending on the same node has been traversed from.
         * @param pathPriority priority of traversal branch currently considered.
         * @param oldPriority priority of other traversal branch.
         * @return
         */
        @Override
        public boolean canBeRuledOut( int numberOfVisits, P pathPriority, P oldPriority )
        {
            return !interestFunction().apply( pathPriority, oldPriority );
        }

        /**
         *
         * @param numberOfVisits
         * @return
         */
        @Override
        public boolean stillInteresting( int numberOfVisits )
        {
            return true;
        }

        /**
         * @return true
         */
        @Override
        public boolean stopAfterLowestCost()
        {
            return true;
        }
    }

    public abstract class VisitCountBasedPathInterest<P> implements PathInterest<P>
    {
        abstract int numberOfWantedPaths();

        /**
         * Use {@link #numberOfWantedPaths()} to decide if an entity should be ruled out or not and if an entity
         * still is of interest.
         * @param numberOfVisits number of times a traversal branch ending on the same node has been traversed from.
         * @param pathPriority priority of traversal branch currently considered.
         * @param oldPriority priority of other traversal branch.
         * @return numberOfVisits > {@link #numberOfWantedPaths()}
         */
        @Override
        public boolean canBeRuledOut( int numberOfVisits, P pathPriority, P oldPriority )
        {
            return numberOfVisits > numberOfWantedPaths();
        }

        /**
         *
         * @param numberOfVisits
         * @return numberOfVisits <= {@link #numberOfWantedPaths()}
         */
        @Override
        public boolean stillInteresting( int numberOfVisits )
        {
            return numberOfVisits <= numberOfWantedPaths();
        }

        /**
         *
         * @return false
         */
        @Override
        public boolean stopAfterLowestCost()
        {
            return false;
        }
    }
}