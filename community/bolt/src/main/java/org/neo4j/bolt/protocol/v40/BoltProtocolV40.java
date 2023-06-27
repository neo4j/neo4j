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
package org.neo4j.bolt.protocol.v40;

import java.util.function.Predicate;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.AbstractBoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPI;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.LegacyMetadataHandler;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultLogoffMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultLogonMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultGoodbyeMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultResetMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultRouteMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.streaming.DefaultDiscardMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.streaming.DefaultPullMessageDecoder;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.protocol.io.reader.DateReader;
import org.neo4j.bolt.protocol.io.reader.DurationReader;
import org.neo4j.bolt.protocol.io.reader.LocalDateTimeReader;
import org.neo4j.bolt.protocol.io.reader.LocalTimeReader;
import org.neo4j.bolt.protocol.io.reader.Point2dReader;
import org.neo4j.bolt.protocol.io.reader.Point3dReader;
import org.neo4j.bolt.protocol.io.reader.TimeReader;
import org.neo4j.bolt.protocol.io.reader.legacy.LegacyDateTimeReader;
import org.neo4j.bolt.protocol.io.reader.legacy.LegacyDateTimeZoneIdReader;
import org.neo4j.bolt.protocol.io.writer.LegacyStructWriter;
import org.neo4j.bolt.protocol.v40.fsm.StateMachineV40;
import org.neo4j.bolt.protocol.v40.message.decoder.authentication.HelloMessageDecoderV40;
import org.neo4j.bolt.protocol.v40.message.decoder.transaction.BeginMessageDecoderV40;
import org.neo4j.bolt.protocol.v40.message.decoder.transaction.RunMessageDecoderV40;
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.storable.Value;

/**
 * Bolt protocol V4. It hosts all the components that are specific to BoltV4
 */
public class BoltProtocolV40 extends AbstractBoltProtocol {
    public static final ProtocolVersion VERSION = new ProtocolVersion(4, 0);

    public BoltProtocolV40(SystemNanoClock clock, LogService logging) {
        super(clock, logging);
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    public Predicate<FrameSignal> frameSignalFilter() {
        // only valid signal in 4.0 was MESSAGE_END - further signals were introduced in later revisions
        return signal -> signal != FrameSignal.MESSAGE_END;
    }

    @Override
    protected StateMachine createStateMachine(Connection connection, StateMachineSPI stateMachineSPI) {
        connection.memoryTracker().allocateHeap(StateMachineV40.SHALLOW_SIZE);

        return new StateMachineV40(stateMachineSPI, connection, clock);
    }

    @Override
    protected StructRegistry.Builder<Connection, RequestMessage> createRequestMessageRegistry() {
        return super.createRequestMessageRegistry()
                // Authentication
                .unregister(DefaultLogonMessageDecoder.getInstance())
                .unregister(DefaultLogoffMessageDecoder.getInstance())
                .register(HelloMessageDecoderV40.getInstance())
                // Connection
                .register(DefaultGoodbyeMessageDecoder.getInstance())
                .register(DefaultResetMessageDecoder.getInstance())
                .register(DefaultRouteMessageDecoder.getInstance())
                // Streaming
                .register(DefaultDiscardMessageDecoder.getInstance())
                .register(DefaultPullMessageDecoder.getInstance())
                // Transaction
                .register(BeginMessageDecoderV40.getInstance())
                .register(RunMessageDecoderV40.getInstance());
    }

    @Override
    @SuppressWarnings("removal")
    public void registerStructReaders(StructRegistry.Builder<Connection, Value> builder) {
        builder.register(DateReader.getInstance())
                .register(DurationReader.getInstance())
                .register(LocalDateTimeReader.getInstance())
                .register(LocalTimeReader.getInstance())
                .register(Point2dReader.getInstance())
                .register(Point3dReader.getInstance())
                .register(TimeReader.getInstance())
                .register(LegacyDateTimeReader.getInstance())
                .register(LegacyDateTimeZoneIdReader.getInstance());
    }

    @Override
    @SuppressWarnings("removal")
    public void registerStructWriters(WriterPipeline pipeline) {
        super.registerStructWriters(pipeline);

        pipeline.addFirst(LegacyStructWriter.getInstance());
    }

    @Override
    public MetadataHandler metadataHandler() {
        return LegacyMetadataHandler.getInstance();
    }
}
