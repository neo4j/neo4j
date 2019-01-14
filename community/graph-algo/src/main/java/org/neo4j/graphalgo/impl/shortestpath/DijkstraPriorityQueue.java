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
package org.neo4j.graphalgo.impl.shortestpath;

import org.neo4j.graphdb.Node;

/**
 * Abstraction of the priority queue used by Dijkstra in order to make (testing
 * of) alternative implementations easier.
 * @param <CostType>
 *            The datatype the path weigths are represented by.
 */
public interface DijkstraPriorityQueue<CostType>
{
    /**
     * Used to insert a new value into the queue.
     * @param node
     * @param value
     */
    void insertValue( Node node, CostType value );

    /**
     * Used to update a value in the queue (or insert it).
     * @param node
     * @param newValue
     */
    void decreaseValue( Node node, CostType newValue );

    /**
     * Retrieve and remove the node with the most optimal value.
     */
    Node extractMin();

    /**
     * Retrieve without removing the node with the most optimal value.
     */
    Node peek();

    /**
     * @return True if the queue is empty.
     */
    boolean isEmpty();
}
