/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.connector.connection;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.Job;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.io.pipeline.PipelineContext;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;

/**
 * Represents an established Bolt connection as well as its current state.
 * <p />
 * This implementation is primarily responsible for keeping track of jobs and their state within the Bolt worker thread
 * pool.
 */
public interface Connection extends TrackedNetworkConnection, TransactionOwner {

    /**
     * Defines a channel attribute which stores the bolt representation assigned to this connection.
     */
    AttributeKey<Connection> CONNECTION_ATTR = AttributeKey.valueOf(Connection.class, "connection");

    /**
     * Retrieves the bolt representation for a given connection.
     * @param channel a connection channel.
     * @return a connection.
     * @throws NullPointerException when no bolt representation has been registered for the given channel.
     */
    static Connection getConnection(Channel channel) {
        var connection = channel.attr(CONNECTION_ATTR).get();
        Objects.requireNonNull(connection, "connection");
        return connection;
    }

    static void setAttribute(Channel channel, Connection connection) {
        channel.attr(CONNECTION_ATTR).set(connection);
    }

    /**
     * Retrieves the Bolt connector which owns this connection.
     *
     * @return a connector.
     */
    Connector connector();

    /**
     * Retrieves the clock which provides the current date and time for operations within the scope
     * of a given connection.
     *
     * @return a clock.
     */
    default Clock clock() {
        return this.connector().clock();
    }

    @Override
    default String connectorId() {
        return this.connector().id();
    }

    /**
     * Retrieves the underlying network channel for this connection.
     *
     * @return a network channel.
     */
    Channel channel();

    /**
     * Shorthand for {@link Channel#write(Object)}
     *
     * @see Channel#write(Object)
     */
    default ChannelFuture write(Object msg) {
        return this.channel().write(msg);
    }

    /**
     * Shorthand for {@link Channel#write(Object, ChannelPromise)}
     *
     * @see Channel#write(Object, ChannelPromise)
     */
    default ChannelFuture write(Object msg, ChannelPromise promise) {
        return this.channel().write(msg, promise);
    }

    /**
     * Shorthand for {@link Channel#writeAndFlush(Object)}
     *
     * @see Channel#writeAndFlush(Object)
     */
    default ChannelFuture writeAndFlush(Object msg) {
        return this.channel().writeAndFlush(msg);
    }

    /**
     * Shorthand for {@link Channel#writeAndFlush(Object, ChannelPromise)}
     *
     * @see Channel#writeAndFlush(Object, ChannelPromise)
     */
    default ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        return this.channel().writeAndFlush(msg, promise);
    }

    /**
     * Shorthand for {@link Channel#flush()}
     *
     * @see Channel#flush()
     */
    default void flush() {
        this.channel().flush();
    }

    /**
     * Registers a new listener with this connection.
     *
     * @param listener a listener.
     */
    void registerListener(ConnectionListener listener);

    /**
     * Removes a listener from this connection.
     *
     * @param listener a listener.
     */
    void removeListener(ConnectionListener listener);

    /**
     * Notifies all registered listeners on this connection.
     *
     * @param notifierFunction a notifier function to be invoked on all listeners.
     */
    void notifyListeners(Consumer<ConnectionListener> notifierFunction);

    /**
     * Notifies all registered listeners on this connection without surfacing errors.
     *
     * @param eventName a descriptive name for the source event.
     * @param notifierFunction a notifier function to be invoked on all listeners.
     */
    void notifyListenersSafely(String eventName, Consumer<ConnectionListener> notifierFunction);

    /**
     * Retrieves the protocol specification used by this connection.
     *
     * @return a protocol or null if none has been selected yet.
     */
    BoltProtocol protocol();

    /**
     * Selects a protocol revision for use with this connection.
     *
     * @param protocol a protocol version.
     *
     * @throws NullPointerException when protocol is null.
     * @throws IllegalStateException when the protocol has already been selected.
     */
    void selectProtocol(BoltProtocol protocol);

    /**
     * Retrieves the value reader which shall be used to parse Packstream values via this connection.
     *
     * @param buf a buffer.
     * @return a packstream value reader.
     */
    PackstreamValueReader<Connection> valueReader(PackstreamBuf buf);

    /**
     * Creates a writer context around a given target buffer.
     *
     * @param buf a buffer.
     * @return a packstream value writer.
     */
    PipelineContext writerContext(PackstreamBuf buf);

    /**
     * Retrieves the finite state machine for this connection.
     *
     * @return a state machine.
     * @throws IllegalStateException when the connection has yet to be negotiated.
     */
    StateMachine fsm();

    /**
     * Retrieves the login context which is currently used by this connection to authenticate operations.
     *
     * @return a login context or null if no authentication has been performed on this connection.
     */
    @Override
    LoginContext loginContext();

    /**
     * Authenticates this connection using a given authentication token.
     *
     * @param token     an authentication token.
     * @return null or an authentication flag which notifies the client about additional requirements or limitations if
     * necessary.
     * @throws AuthenticationException when the given token is invalid or authentication fails.
     * @see AuthenticationFlag for detailed information on the available authentication flags.
     */
    AuthenticationFlag logon(Map<String, Object> token) throws AuthenticationException;

    /**
     * Logs off this connection, so it is ready to accept new authentication.
     */
    void logoff();

    /**
     * Impersonates a given target user.
     *
     * @param userToImpersonate the name of the user to impersonate or null.
     * @throws AuthenticationException when the given user cannot be impersonated by the current user.
     */
    void impersonate(String userToImpersonate) throws AuthenticationException;

    /**
     * Clears any previously configured {@link #impersonate(String) impersonation}.
     * <p />
     * When no impersonation has been configured, this method acts as a NOOP.
     */
    void clearImpersonation();

    /**
     * Resolves the default database which shall be used in absence of an explicitly selected database.
     */
    void resolveDefaultDatabase();

    /**
     * Evaluates whether this connection is currently idling.
     * <p />
     * A connection is considered in idle when it does not currently have an active transaction, is not
     * executing jobs and has no jobs remaining in its queue.
     *
     * @return true if idling, false otherwise.
     */
    boolean isIdling();

    /**
     * Checks whether this connection has remaining queued jobs which are pending execution.
     *
     * @return true if one or more jobs remain, false otherwsie.
     */
    boolean hasPendingJobs();

    /**
     * Attempts to submit a new request to the connection execution queue.
     * <p />
     *
     * @see #submit(Job) for more information on job scheduling.
     * @param message an arbitrary request message as defined by the remote peer.
     */
    void submit(RequestMessage message);

    /**
     * Attempts to submit a new job to the connection execution queue.
     * <p />
     * When no jobs are currently queued on this connection, the connection will be scheduled for execution on the next
     * available worker thread.
     *
     * @param job an arbitrary job as defined by the remote peer.
     */
    void submit(Job job);

    /**
     * Evaluates whether the function is invoked from the worker thread currently assigned to this connection.
     * <p />
     * When no thread is currently executing tasks for this connection (e.g. there are no tasks, or no thread has been
     * assigned yet), this function will always return false.
     *
     * @return true if invoked from the worker thread, false otherwise.
     */
    boolean inWorkerThread();

    /**
     * Checks whether this connection is currently interrupted and waiting for the client to reset it to its original
     * state.
     * <p />
     * Connections are interrupted when a {@code RESET} message is received within one of the network threads. All
     * remaining requests within the queue will be ignored until the corresponding {@code RESET} is processed within the
     * state machine.
     * <p />
     * If multiple {@code RESET} messages are received, their effects "stack" (e.g. the connection remains interrupted
     * until <b>ALL</b> instances of {@code RESET} have been processed.
     *
     * @return true if interrupted, false otherwise.
     */
    boolean isInterrupted();

    /**
     * Interrupts this connection and aborts any currently active jobs if possible.
     * <p />
     * Once interrupted, connections will ignore any jobs (both queued and newly submitted) until it is {@link #reset}
     * to a valid state.
     */
    void interrupt();

    /**
     * Resets the connection to its initial state if possible.
     *
     * @return true if reset to a valid state, false otherwise.
     */
    boolean reset();

    /**
     * Evaluates whether this connection is currently considered active (e.g. has not been marked for closure or
     * actually closed).
     *
     * @return true if active, false otherwise.
     */
    boolean isActive();

    /**
     * Checks whether this connection has been marked for closure.
     * <p />
     * Returns true when the underlying connection is marked for termination (e.g. due to a network-side disconnect or
     * forceful termination on the server side).
     *
     * @return true if closing, false otherwise.
     */
    boolean isClosing();

    /**
     *  Checks whether this connection has been closed.
     *
     * @return true if closed, false otherwise.
     */
    boolean isClosed();

    /**
     * Closes this connection and all of its associated resources.
     * <p />
     * When invoked from the bolt worker thread which is currently handling this connection, or the connection is
     * currently idling (e.g. has no ongoing or scheduled jobs), the connection will be terminated immediately from the
     * calling thread.
     * <p />
     * When invoked from any other thread, the connection will be marked for closure at the next possible
     * opportunity (e.g. when its current scheduled task terminates).
     * <p />
     * This function will also attempt to interrupt any remaining operating transactions within this connection if
     * applicable (close implies {@link #interrupt()}).
     */
    @Override
    void close();

    /**
     * Returns a future which is completed when this connection is finally closed.
     *
     * @return a future.
     */
    Future<?> closeFuture();

    /**
     * Provides a factory capable of constructing a connection implementation for a given network channel and finite
     * state machine.
     * <p />
     * This specification is primarily provided for convenience as connections typically hold references to a variety of
     * dependencies which would otherwise be cumbersome to pass through the stack.
     */
    interface Factory {

        /**
         * Creates a new connection for the given network channel and finite state machine.
         *
         * @param connector a connector which owns the connection.
         * @param id a connection identifier.
         * @param channel a network channel.
         * @return a connection.
         */
        Connection create(Connector connector, String id, Channel channel);
    }
}
