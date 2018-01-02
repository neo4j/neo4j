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
package org.neo4j.unsafe.impl.batchimport;

/**
 * Represents something that can be parallelizable, in this case that means the ability to dynamically change
 * the number of processors executing that tasks ahead.
 */
public interface Parallelizable
{
    /**
     * The number of processors processing incoming tasks in parallel.
     */
    int numberOfProcessors();

    /**
     * Increments number of processors that processes tasks in parallel.
     *
     * @return {@code true} if one more processor was assigned, otherwise {@code false}.
     */
    boolean incrementNumberOfProcessors();

    /**
     * Decrements number of processors that processes tasks in parallel.
     *
     * @return {@code true} if one processor was unassigned, otherwise {@code false}.
     */
    boolean decrementNumberOfProcessors();
}
