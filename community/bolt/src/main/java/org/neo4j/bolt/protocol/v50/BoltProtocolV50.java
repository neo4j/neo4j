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
package org.neo4j.bolt.protocol.v50;

import org.neo4j.bolt.fsm.StateMachineConfiguration.Factory;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.AbstractBoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.fsm.transition.authentication.AuthenticationStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.negotiation.HelloStateTransition;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultLogoffMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultLogonMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.generic.TelemetryMessageDecoder;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.protocol.io.writer.DefaultStructWriter;
import org.neo4j.bolt.protocol.v41.message.decoder.authentication.HelloMessageDecoderV41;
import org.neo4j.bolt.protocol.v44.fsm.response.metadata.MetadataHandlerV44;
import org.neo4j.bolt.protocol.v44.message.decoder.transaction.RunMessageDecoderV44;
import org.neo4j.bolt.protocol.v50.message.decoder.transaction.BeginMessageDecoderV50;
import org.neo4j.packstream.struct.StructRegistry;

public final class BoltProtocolV50 extends AbstractBoltProtocol {
    private static final BoltProtocolV50 INSTANCE = new BoltProtocolV50();
    public static final ProtocolVersion VERSION = new ProtocolVersion(5, 0);

    public static BoltProtocolV50 getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    protected Factory createStateMachine() {
        // 5.0 is the only version which does not support dropping a connection back to its
        // un-authenticated state thus requiring separate configuration within this version in
        // order to restore 4.x authentication behavior
        return super.createStateMachine()
                .withoutState(States.NEGOTIATION)
                .withInitialState(
                        States.AUTHENTICATION,
                        HelloStateTransition.getInstance().andThen(AuthenticationStateTransition.getInstance()));
    }

    @Override
    public void registerStructWriters(WriterPipeline pipeline) {
        pipeline.addLast(DefaultStructWriter.getInstance());
    }

    @Override
    protected StructRegistry.Builder<Connection, RequestMessage> createRequestMessageRegistry() {
        return super.createRequestMessageRegistry()
                .unregister(DefaultLogonMessageDecoder.getInstance())
                .unregister(DefaultLogoffMessageDecoder.getInstance())
                .register(HelloMessageDecoderV41.getInstance())
                .register(BeginMessageDecoderV50.getInstance())
                .register(RunMessageDecoderV44.getInstance())
                // Generic
                .unregister(TelemetryMessageDecoder.getInstance());
    }

    @Override
    public MetadataHandler metadataHandler() {
        return MetadataHandlerV44.getInstance();
    }
}
