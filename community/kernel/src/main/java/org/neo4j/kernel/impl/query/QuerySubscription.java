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
package org.neo4j.kernel.impl.query;

/**
 * A QuerySubscription is used to stream data from a query.
 *
 * Used in conjunction with a {@link QuerySubscriber}. The client demands a number of records via {@link #request(long)},
 * data will then be streamed to the subscriber until the demand is met or the there is no more data to stream.
 */
public interface QuerySubscription
{
    /**
     * Request a number of records.
     * @param numberOfRecords The number of records demanded.
     */
    void request( long numberOfRecords ) throws Exception;

    /**
     * Request the query to not send any more data.
     */
    void cancel();

    /**
     * Synchronously await until all the demanded records have been streamed to the {@link QuerySubscriber}
     *
     * @return <code>true</code> if there could be more data in the stream (when request exactly equals available data this method may return true),
     * otherwise <code>false</code>
     */
    boolean await() throws Exception;

    /**
     * Consumes all results.
     */
    default void consumeAll() throws Exception
    {
        request( Long.MAX_VALUE );
        await();
    }
}
