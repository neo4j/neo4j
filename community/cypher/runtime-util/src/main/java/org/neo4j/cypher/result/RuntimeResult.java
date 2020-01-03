/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.result;

import org.neo4j.cypher.internal.runtime.QueryStatistics;
import org.neo4j.graphdb.ResourceIterator;

/**
 * The result API of a Cypher runtime
 */
public interface RuntimeResult extends AutoCloseable
{
    enum ConsumptionState
    {
        NOT_STARTED,
        HAS_MORE,
        EXHAUSTED
    };

    /**
     * Names of the returned fields of this result.
     */
    String[] fieldNames();

    /**
     * True if this result can be consumed as an iterator. See {@link RuntimeResult#asIterator}.
     */
    boolean isIterable();

    /**
     * Consume this result as an iterator. Will complain if {@link RuntimeResult#isIterable()} is false.
     */
    ResourceIterator<java.util.Map<String, Object>> asIterator();

    /**
     * Returns the consumption state of this result. This state changes when the result is served
     * either via {@link RuntimeResult#asIterator} or {@link RuntimeResult#accept(QueryResult.QueryResultVisitor)}.
     */
    ConsumptionState consumptionState();

    /**
     * Consume this result using a visitor.
     */
    <E extends Exception> void accept( QueryResult.QueryResultVisitor<E> visitor )
            throws E;

    /**
     * Get the {@link QueryStatistics} related to this query execution.
     */
    QueryStatistics queryStatistics();

    /**
     * Get the {@link QueryProfile} of this query execution.
     */
    QueryProfile queryProfile();

    @Override
    void close();
}
