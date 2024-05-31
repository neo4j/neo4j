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
package org.neo4j.bolt.protocol.common.connector.connection.listener;

import io.netty.channel.ChannelPipeline;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.values.virtual.MapValue;

/**
 * Provides hooks for various events within the connection lifecycle.
 * <p />
 * Implementors should generally avoid throwing exceptions as these directly bubble up into the connection handling
 * architecture unless otherwise defined.
 */
public interface ConnectionListener {

    /**
     * Handles the addition of this listener to a given connection.
     */
    default void onListenerAdded() {}

    /**
     * Handles the removal of this listener from a given connection.
     */
    default void onListenerRemoved() {}

    /**
     * Handles the creation of the network pipeline at the end of the channel initialization.
     * <p />
     * This event occurs exactly once when the channel is first introduced by the network stack after all standard Bolt
     * handlers have been installed within the pipeline.
     *
     * @param pipeline a network pipeline.
     */
    default void onNetworkPipelineInitialized(ChannelPipeline pipeline) {}

    /**
     * Handles the initialization of the state machine.
     * <p />
     * This event occurs exactly once when the connection state machine has been initialized.
     *
     * @param fsm a finite state machine.
     */
    default void onStateMachineInitialized(StateMachine fsm) {}

    /**
     * Handles the selection of the desired protocol version.
     * <p />
     * This event occurs exactly once when the channel has selected its desired Bolt protocol version after all related
     * handlers have been installed within the pipeline.
     *
     * @param protocol a protocol version.
     */
    default void onProtocolSelected(BoltProtocol protocol) {}

    /**
     * Handles the scheduling of the connection.
     * <p />
     * This event occurs whenever the connection switches from being in idle to being scheduled for execution.
     */
    default void onScheduled() {}

    /**
     * Handles the activation of the connection.
     * <p />
     * This event occurs whenever the connection transitions from being scheduled to actively processing requests.
     */
    default void onActivated() {}

    /**
     * Handles the de-activation of the connection.
     * <p />
     * This event occurs whenever the connection transitions from being actively processing requests to idling.
     */
    default void onIdle(long boundTimeMillis) {}

    /**
     * Handles the submission of a request for processing within the connection.
     *
     * @param message a message.
     */
    default void onRequestReceived(RequestMessage message) {}

    /**
     * Handles the beginning of processing for a given request message.
     *
     * @param message a message.
     * @param queuedForMillis the amount of milliseconds that this message spent within the queue.
     */
    default void onRequestBeginProcessing(RequestMessage message, long queuedForMillis) {}

    /**
     * Handles the completion of processing for a given request message.
     *
     * @param message a message.
     * @param processedForMillis the amount of milliseconds that this message took to be processed.
     */
    default void onRequestCompletedProcessing(RequestMessage message, long processedForMillis) {}

    /**
     * Handles the completion of processing by failure for a given request message.
     *
     * @param message a message.
     * @param cause the cause for the failure.
     */
    default void onRequestFailedProcessing(RequestMessage message, Throwable cause) {}

    /**
     * Handles the completed authentication of the connection.
     *
     * @param ctx a new login context.
     */
    default void onLogon(LoginContext ctx) {}

    /**
     * Handles logging off
     */
    default void onLogoff() {}

    /**
     * Handles the selected impersonation of the connection.
     *
     * @param ctx an impersonation login context.
     */
    default void onUserImpersonated(LoginContext ctx) {}

    /**
     * Handles the end of impersonation of the connection.
     */
    default void onUserImpersonationCleared() {}

    /**
     * Handles the selection of a default database for the connection.
     *
     * @param db a database name.
     */
    default void onDefaultDatabaseSelected(String db) {}

    /**
     * Handles a successful result for a given request.
     */
    default void onResponseSuccess(MapValue metadata) {}

    /**
     * Handles a failure result for a given request.
     *
     * @param error a status code.
     */
    default void onResponseFailed(Error error) {}

    /**
     * Handles an ignored result for a given request.
     */
    default void onResponseIgnored() {}

    /**
     * Handles the scheduled closure of the connection.
     * <p />
     * This function is invoked exactly once within the lifetime of a connection.
     */
    default void onMarkedForClosure() {}

    /**
     * Handles the closure of the connection.
     * <p />
     * This function is invoked exactly once within the lifetime of a connection.
     */
    default void onConnectionClosed(boolean isNegotiatedConnection) {}
}
