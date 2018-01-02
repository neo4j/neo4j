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
package org.neo4j.bolt.v1.runtime;

import java.util.Map;

import org.neo4j.bolt.v1.runtime.internal.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;

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

        static <V,A> Callback<V,A> noOp() {
            return NO_OP;
        }

        /** Called exactly once, before the request is processed by the Session State Machine */
        void started( A attachment );

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
            public void started( A attachment )
            {
                // this page intentionally left blank
            }

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

    /** A descriptor for the underlying medium (connection etc) via which this session is being used */
    String connectionDescriptor();

    /**
     * Initialize the session.
     */
    <A> void init( String clientName, Map<String,Object> authToken, A attachment, Callback<Boolean,A> callback );

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
     * Clear any outstanding error condition. This differs from {@link #reset(Object, Callback)} in two
     * important ways:
     *
     * 1) If there was an explicitly created transaction, the session will move back
     *    to IN_TRANSACTION, rather than IDLE. This allows a more natural flow for client
     *    side drivers, where explicitly opened transactions always are ended with COMMIT or ROLLBACK,
     *    even if an error occurs. In all other cases, the session will move to the IDLE state.
     *
     * 2) It will not interrupt any ahead-in-line messages.
     */
    <A> void ackFailure( A attachment, Callback<Void,A> callback );


    /**
     * Reset the session to an IDLE state. This clears any outstanding failure condition, disposes
     * of any outstanding result records and rolls back the current transaction (if any).
     *
     * This differs from {@link #reset(Object, Callback)} in that it is more "radical" - it does not
     * matter what the state of the session is, as long as it is open, reset will move it back to IDLE.
     *
     * This is designed to cater to two use cases:
     *
     * 1) Rather than create new sessions over and over, drivers can maintain a pool of sessions,
     *    and reset them before each re-use. Since establishing sessions can be high overhead,
     *    this is quite helpful.
     * 2) Kill any stuck or long running operation
     */
    <A> void reset( A attachment, Callback<Void,A> callback );

    /**
     * Signals that the infrastructure around the session has failed in some non-recoverable way; it will
     * not be able to deliver more messages to the session. This is meant to allow the session to signal
     * back out any final message it wants delivered.
     *
     * Note that this does not close the session; close can be expected to be called immediately after
     * this call.
     *
     * @param error cause of the fatal error
     */
    <A> void externalError( Neo4jError error, A attachment, Callback<Void,A> callback  );

    /**
     * This is a special mechanism, it is the only method on this interface
     * that is thread safe. When this is invoked, the machine will make attempts
     * at interrupting any currently running action,
     * and will then ignore all inbound messages until a {@link #reset(Object, Callback) reset}
     * message is received. If this is called multiple times, an equivalent number
     * of reset messages must be received before the SSM goes back to a good state.
     *
     * You can imagine this is as a "call ahead" mechanism used by RESET to
     * cancel any statements ahead of it in line, without compromising the single-
     * threaded processing of messages that the state machine does.
     *
     * This can be used to cancel a long-running statement or transaction.
     */
    void interrupt();

    @Override
    void close();
}
