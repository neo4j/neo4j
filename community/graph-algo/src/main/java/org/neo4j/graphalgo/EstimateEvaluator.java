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

/**
 * Evaluator used to estimate the weight of the remaining path from one node to
 * another.
 *
 * @author Mattias Persson
 * @param <T> The data type of the estimated weight.
 */
public interface EstimateEvaluator<T>
{
    /**
     * Estimate the weight of the remaining path from one node to another.
     *
     * @param node the node to estimate the weight from.
     * @param goal the node to estimate the weight to.
     * @return an estimation of the weight of the path from the first node to
     *         the second.
     */
    T getCost( Node node, Node goal );
}
