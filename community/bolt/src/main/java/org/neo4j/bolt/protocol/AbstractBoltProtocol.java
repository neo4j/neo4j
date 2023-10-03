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
package org.neo4j.bolt.protocol;

import org.neo4j.bolt.fsm.StateMachineConfiguration;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.transition.authentication.AuthenticationStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.authentication.LogoffStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.negotiation.HelloStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.CreateAutocommitStatementStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.CreateTransactionStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.RouteStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.TelemetryStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.CommitTransactionalStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.CreateStatementStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.RollbackTransactionalStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.streaming.AutocommitDiscardStreamingStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.streaming.AutocommitPullStreamingStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.streaming.DiscardResultsStreamingStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.streaming.PullResultsStreamingStateTransition;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultHelloMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultLogoffMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultLogonMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultGoodbyeMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultResetMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultRouteMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.generic.TelemetryMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.streaming.DefaultDiscardMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.streaming.DefaultPullMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultBeginMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultCommitMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultRollbackMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultRunMessageDecoder;
import org.neo4j.bolt.protocol.common.message.encoder.FailureMessageEncoder;
import org.neo4j.bolt.protocol.common.message.encoder.IgnoredMessageEncoder;
import org.neo4j.bolt.protocol.common.message.encoder.SuccessMessageEncoder;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.packstream.struct.StructRegistry;

public abstract class AbstractBoltProtocol implements BoltProtocol {

    private final StateMachineConfiguration stateMachine;
    private final StructRegistry<Connection, RequestMessage> requestMessageStructRegistry;
    private final StructRegistry<Connection, ResponseMessage> responseMessageStructRegistry;

    protected AbstractBoltProtocol() {
        this.stateMachine = this.createStateMachine().build();
        this.requestMessageStructRegistry = this.createRequestMessageRegistry().build();
        this.responseMessageStructRegistry =
                this.createResponseMessageRegistry().build();
    }

    @Override
    public final StructRegistry<Connection, RequestMessage> requestMessageRegistry() {
        return this.requestMessageStructRegistry;
    }

    @Override
    public final StructRegistry<Connection, ResponseMessage> responseMessageRegistry() {
        return this.responseMessageStructRegistry;
    }

    @Override
    public StateMachineConfiguration stateMachine() {
        return this.stateMachine;
    }

    protected StateMachineConfiguration.Factory createStateMachine() {
        return StateMachineConfiguration.builder()
                .withInitialState(States.NEGOTIATION, HelloStateTransition.getInstance())
                .withState(States.AUTHENTICATION, AuthenticationStateTransition.getInstance())
                .withState(
                        States.READY,
                        CreateTransactionStateTransition.getInstance(),
                        RouteStateTransition.getInstance(),
                        CreateAutocommitStatementStateTransition.getInstance(),
                        LogoffStateTransition.getInstance(),
                        TelemetryStateTransition.getInstance())
                .withState(
                        States.AUTO_COMMIT,
                        AutocommitDiscardStreamingStateTransition.getInstance(),
                        AutocommitPullStreamingStateTransition.getInstance())
                .withState(
                        States.IN_TRANSACTION,
                        CreateStatementStateTransition.getInstance(),
                        DiscardResultsStreamingStateTransition.getInstance(),
                        PullResultsStreamingStateTransition.getInstance(),
                        CommitTransactionalStateTransition.getInstance(),
                        RollbackTransactionalStateTransition.getInstance());
    }

    protected StructRegistry.Builder<Connection, RequestMessage> createRequestMessageRegistry() {
        return StructRegistry.<Connection, RequestMessage>builder()
                // Authentication
                .register(DefaultHelloMessageDecoder.getInstance())
                .register(DefaultLogonMessageDecoder.getInstance())
                .register(DefaultLogoffMessageDecoder.getInstance())
                // Connection
                .register(DefaultGoodbyeMessageDecoder.getInstance())
                .register(DefaultResetMessageDecoder.getInstance())
                .register(DefaultRouteMessageDecoder.getInstance())
                // Streaming
                .register(DefaultDiscardMessageDecoder.getInstance())
                .register(DefaultPullMessageDecoder.getInstance())
                // Transaction
                .register(DefaultBeginMessageDecoder.getInstance())
                .register(DefaultCommitMessageDecoder.getInstance())
                .register(DefaultRollbackMessageDecoder.getInstance())
                .register(DefaultRunMessageDecoder.getInstance())
                // Generic
                .register(TelemetryMessageDecoder.getInstance());
    }

    protected StructRegistry.Builder<Connection, ResponseMessage> createResponseMessageRegistry() {
        return StructRegistry.<Connection, ResponseMessage>builder()
                .register(FailureMessageEncoder.getInstance())
                .register(IgnoredMessageEncoder.getInstance())
                .register(SuccessMessageEncoder.getInstance());
    }

    @Override
    public String toString() {
        return this.version().toString();
    }

    @Override
    public ConnectionHintProvider connectionHintProvider() {
        return ConnectionHintProvider.noop();
    }
}
