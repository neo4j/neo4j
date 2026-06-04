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
package org.neo4j.bolt.protocol.v56;

import org.neo4j.bolt.fsm.StateMachineConfiguration;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.AbstractBoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.fsm.transition.authentication.AuthenticationStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.authentication.LogoffStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.CreateAutocommitStatementStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.CreateTransactionStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.RouteStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.TelemetryStateTransition;
import org.neo4j.bolt.protocol.common.message.encoder.FailureMessageEncoder;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.protocol.io.reader.struct.DateReader;
import org.neo4j.bolt.protocol.io.reader.struct.DateTimeReader;
import org.neo4j.bolt.protocol.io.reader.struct.DateTimeZoneIdReader;
import org.neo4j.bolt.protocol.io.reader.struct.DurationReader;
import org.neo4j.bolt.protocol.io.reader.struct.LocalDateTimeReader;
import org.neo4j.bolt.protocol.io.reader.struct.LocalTimeReader;
import org.neo4j.bolt.protocol.io.reader.struct.Point2dReader;
import org.neo4j.bolt.protocol.io.reader.struct.Point3dReader;
import org.neo4j.bolt.protocol.io.reader.struct.TimeReader;
import org.neo4j.bolt.protocol.io.writer.UUIDUnknownTypeVersionedValueWriter;
import org.neo4j.bolt.protocol.io.writer.VectorUnknownTypeVersionedValueWriter;
import org.neo4j.bolt.protocol.v40.message.encoder.FailureMessageEncoderV40;
import org.neo4j.bolt.protocol.v56.metadata.MetadataHandlerV56;
import org.neo4j.boltmessages.response.ResponseMessage;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.values.storable.Value;

public final class BoltProtocolV56 extends AbstractBoltProtocol {
    public static final ProtocolVersion VERSION = new ProtocolVersion(5, 6);

    private static final BoltProtocolV56 INSTANCE = new BoltProtocolV56();

    private BoltProtocolV56() {}

    public static BoltProtocolV56 getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    protected StructRegistry.Builder<Connection, ResponseMessage> createResponseMessageRegistry() {
        return super.createResponseMessageRegistry()
                .unregister(FailureMessageEncoder.getInstance())
                .register(FailureMessageEncoderV40.getInstance());
    }

    @Override
    protected StateMachineConfiguration.Factory createStateMachine() {
        return super.createStateMachine()
                .withState(States.AUTHENTICATION, AuthenticationStateTransition.getInstance())
                .withState(
                        States.READY,
                        CreateTransactionStateTransition.getInstance(),
                        RouteStateTransition.getInstance(),
                        CreateAutocommitStatementStateTransition.getInstance(),
                        LogoffStateTransition.getInstance(),
                        TelemetryStateTransition.getInstance());
    }

    @Override
    public void registerStructReaders(StructRegistry.Builder<Connection, Value> builder) {
        builder.register(DateReader.getInstance())
                .register(DurationReader.getInstance())
                .register(LocalDateTimeReader.getInstance())
                .register(LocalTimeReader.getInstance())
                .register(Point2dReader.getInstance())
                .register(Point3dReader.getInstance())
                .register(TimeReader.getInstance())
                .register(DateTimeReader.getInstance())
                .register(DateTimeZoneIdReader.getInstance());
    }

    @Override
    @SuppressWarnings("removal")
    public void registerStructWriters(WriterPipeline pipeline) {
        pipeline.addLast(VectorUnknownTypeVersionedValueWriter.getInstance())
                .addLast(UUIDUnknownTypeVersionedValueWriter.getInstance());

        super.registerStructWriters(pipeline);
    }

    @Override
    public MetadataHandler metadataHandler() {
        return MetadataHandlerV56.getInstance();
    }

    @Override
    public boolean supportsPackstreamType(Type type) {
        return type != Type.UUID;
    }
}
