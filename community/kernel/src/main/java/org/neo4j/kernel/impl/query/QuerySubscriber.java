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
package org.neo4j.kernel.impl.query;

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;

/**
 * A QuerySubscriber is used for streaming result from a query.
 * <p>
 * Used in conjunction with a {@link QuerySubscription}. The subscription will demand a number of records, {@link
 * QuerySubscription#request(long)}, and when there is data available the subscriber will receive a call chain like.
 *
 <pre>
 *     onResult(3)
 *     onRecord()
 *     onField(0, v1)
 *     onField(1, v2)
 *     onField(2, v3)
 *     onRecordCompleted()
 *     onRecord()
 *     onField(0, v1)
 *     onField(1, v2)
 *     onField(2, v3)
 *     onRecordCompleted()
 *     ...
 *     onResultCompleted(stats)
 * </pre>
 * <p>
 * Or if some error occurs along the way we will have a call to onError.
 */
public interface QuerySubscriber
{
    /**
     * Called at the beginning of a stream, guaranteed to be called exactly once.
     *
     * @param numberOfFields the number of fields each record of the stream will contain
     */
    void onResult( int numberOfFields ) throws Exception;

    /**
     * Called whenever a new record is ready to be written
     */
    void onRecord() throws Exception;

    /**
     * Writes the field.
     * <p>
     * This method is guaranteed to be called exactly <tt>numberOfFields</tt> times with increasing offsets in [0,
     * <tt>numberOfFields</tt> -1],
     * where <tt>numberOfFields</tt> is the argument provided in {@link #onResult(int)} ()}
     *
     * @param value the value of the field at the current offset.
     */
    void onField( int offset, AnyValue value ) throws Exception;

    /**
     * The current record has been completed
     */
    void onRecordCompleted() throws Exception;

    /**
     * Called if an error occurs
     *
     * @param throwable the error
     */
    void onError( Throwable throwable ) throws Exception;

    /**
     * Call onError while suppressing any new exception.
     */
    static void safelyOnError( QuerySubscriber subscriber, Throwable t )
    {
        try
        {
            subscriber.onError( t );
        }
        catch ( Exception onErrorException )
        {
            t.addSuppressed( onErrorException );
        }
    }

    /**
     * The result stream is done, no more data to stream.
     *
     * This means that further calls {@link QuerySubscription#request(long)} will not result in more data being
     * streamed to the subscriber.
     * @param statistics The query statistics of the results.
     */
    void onResultCompleted( QueryStatistics statistics );

    /**
     * Dummy implementation that will throw whenever it is being called.
     */
    QuerySubscriber DO_NOTHING_SUBSCRIBER = new QuerySubscriber()
    {
        @Override
        public void onResult( int numberOfFields )
        {

        }

        @Override
        public void onRecord()
        {

        }

        @Override
        public void onField( int offset, AnyValue value )
        {

        }

        @Override
        public void onRecordCompleted()
        {

        }

        @Override
        public void onError( Throwable throwable )
        {

        }

        @Override
        public void onResultCompleted( QueryStatistics statistics )
        {

        }
    };
}
