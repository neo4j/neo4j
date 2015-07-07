/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.runtime;

import java.util.Map;

import org.neo4j.ndp.runtime.internal.Neo4jError;
import org.neo4j.ndp.runtime.spi.RecordStream;

/**
 * A user session associated with a given {@link Sessions}. The majority of methods on this
 * interface are asynchronous, requesting that you provide a {@link Session.Callback} to be used to
 * publish the result.
 * <p/>
 * This arrangement reflects the asynchronous nature of sessions and enables several features. This includes
 * out-of-band
 * messages such as warnings, forwarding statements over the network, and ignoring messages until errors have been
 * acknowledged by the client.
 * <p/>
 * While the operations are asynchronous, they are guaranteed to be executed in calling order. This allows you to call
 * several operations in sequence without waiting for the previous operation to complete.
 */
public interface Session extends AutoCloseable
{
    /**
     * Callback for handling the result of requests. For a given session, callbacks will be invoked serially,
     * in the order they were given. This means you may pass the same callback multiple times without waiting for a
     * reply, and are guaranteed that your callbacks will be called in order.
     *
     * @param <V>
     * @param <A>
     */
    interface Callback<V, A>
    {
        Callback NO_OP = new Adapter()
        {
        };

        /** Called zero or more times with results, if the operation invoked yields results. */
        void result( V result, A attachment ) throws Exception;

        /** Called zero or more times if there are failures */
        void failure( Neo4jError err, A attachment );

        /** Called when the operation is completed. */
        void completed( A attachment );

        /** Called when the state machine ignores an operation, because it is waiting for an error to be acknowledged */
        void ignored( A attachment );

        abstract class Adapter<V, A> implements Callback<V,A>
        {
            @Override
            public void result( V result, A attachment ) throws Exception
            {
                // this page intentionally left blank
            }

            @Override
            public void failure( Neo4jError err, A attachment )
            {
                // this page intentionally left blank
            }

            @Override
            public void completed( A attachment )
            {
                // this page intentionally left blank
            }

            @Override
            public void ignored( A attachment )
            {
                // this page intentionally left blank
            }
        }
    }

    class Callbacks
    {
        private Callbacks()
        {
        }

        @SuppressWarnings( "unchecked" )
        public static <V, A> Callback<V,A> noop()
        {
            return (Callback<V,A>) Callback.NO_OP;
        }
    }

    /** A session id that is unique for this database instance */
    String key();

    /**
     * Initialize the session.
     */
    <A> void initialize( String clientName, A attachment, Callback<Void,A> callback );

    /**
     * Run a statement, yielding a result stream which can be retrieved through pulling it in a subsequent call.
     * <p/>
     * If there is a statement running already, all remaining items in its stream must be {@link #pullAll(Object,
     * Session.Callback) pulled} or {@link #discardAll(Object, Session.Callback)
     * discarded}.
     */
    <A> void run( String statement, Map<String,Object> params, A attachment, Callback<StatementMetadata,A> callback );

    /**
     * Retrieve all remaining entries in the current result. This is a distinct operation from 'run' in order to
     * enable pulling the output stream in chunks controlled by the user
     */
    <A> void pullAll( A attachment, Callback<RecordStream,A> callback );

    /**
     * Discard all the remaining entries in the current result stream. This has the same semantic behavior as
     * {@link #pullAll(Object, Session.Callback)}, but without actually retrieving the stream.
     * This is useful for completing the run of a statement when you don't care about the data result.
     */
    <A> void discardAll( A attachment, Callback<Void,A> callback );

    /**
     * Whenever an error has occurred, all incoming requests will be ignored until the error is acknowledged through
     * this method. The point of this is that we can do pipelining, sending multiple requests in one go and
     * optimistically assuming they will succeed. If any of them fail all subsequent requests are declined until the
     * client has acknowledged it has seen the error and has taken it into account for upcoming requests.
     * <p/>
     * Whenever an error has been acknowledged, the session will revert back to its intial state. Any ongoing
     * statements
     * or transactions will have been rolled back and/or disposed of.
     */
    <A> void acknowledgeFailure( A attachment, Callback<Void,A> callback );

    @Override
    void close();
}
