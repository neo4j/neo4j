/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.protocol;

import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPI;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPIImpl;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultHelloMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultLogoffMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultLogonMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultGoodbyeMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultResetMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultRouteMessageDecoder;
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
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;

public abstract class AbstractBoltProtocol implements BoltProtocol {

    protected final SystemNanoClock clock;
    protected final LogService logging;

    private final StructRegistry<Connection, RequestMessage> requestMessageStructRegistry;
    private final StructRegistry<Connection, ResponseMessage> responseMessageStructRegistry;

    protected AbstractBoltProtocol(SystemNanoClock clock, LogService logging) {
        this.clock = clock;
        this.logging = logging;

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
    public final StateMachine createStateMachine(Connection connection) {
        var stateMachineSPI = this.createStateMachineSPI(connection);
        return this.createStateMachine(connection, stateMachineSPI);
    }

    protected StateMachineSPI createStateMachineSPI(Connection connection) {
        connection.memoryTracker().allocateHeap(StateMachineSPIImpl.SHALLOW_SIZE);

        return new StateMachineSPIImpl(logging);
    }

    protected abstract StateMachine createStateMachine(Connection connection, StateMachineSPI stateMachineSPI);

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
                .register(DefaultRunMessageDecoder.getInstance());
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
}
