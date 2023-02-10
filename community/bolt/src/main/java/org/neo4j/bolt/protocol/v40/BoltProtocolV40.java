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
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPIImpl;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.LegacyMetadataHandler;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.message.encoder.FailureMessageEncoder;
import org.neo4j.bolt.protocol.common.message.encoder.IgnoredMessageEncoder;
import org.neo4j.bolt.protocol.common.message.encoder.SuccessMessageEncoder;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
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
import org.neo4j.bolt.protocol.v40.messaging.decoder.BeginMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.CommitMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.DiscardMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.GoodbyeMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.HelloMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.PullMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.ResetMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.RollbackMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.RunMessageDecoder;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.storable.Value;

/**
 * Bolt protocol V4. It hosts all the components that are specific to BoltV4
 */
public class BoltProtocolV40 implements BoltProtocol {
    public static final ProtocolVersion VERSION = new ProtocolVersion(4, 0);

    protected final LogService logging;
    protected final Log log;

    protected final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    protected final SystemNanoClock clock;

    private final StructRegistry<Connection, RequestMessage> requestMessageStructRegistry;
    private final StructRegistry<Connection, ResponseMessage> responseMessageStructRegistry;

    public BoltProtocolV40(
            LogService logging,
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            SystemNanoClock clock) {
        this.logging = logging;
        this.log = logging.getInternalLog(getClass());

        this.boltGraphDatabaseManagementServiceSPI = boltGraphDatabaseManagementServiceSPI;
        this.clock = clock;

        this.requestMessageStructRegistry = this.createRequestMessageRegistry();
        this.responseMessageStructRegistry = this.createResponseMessageRegistry();
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
    public StateMachine createStateMachine(Connection connection) {
        connection.memoryTracker().allocateHeap(StateMachineSPIImpl.SHALLOW_SIZE + StateMachineV40.SHALLOW_SIZE);

        var boltSPI = new StateMachineSPIImpl(logging);

        return new StateMachineV40(boltSPI, connection, clock);
    }

    protected StructRegistry<Connection, RequestMessage> createRequestMessageRegistry() {
        return StructRegistry.<Connection, RequestMessage>builder()
                .register(HelloMessageDecoder.getInstance()) // since 3.x
                .register(RunMessageDecoder.getInstance())
                .register(DiscardMessageDecoder.getInstance())
                .register(PullMessageDecoder.getInstance())
                .register(BeginMessageDecoder.getInstance())
                .register(CommitMessageDecoder.getInstance()) // since 3.x
                .register(RollbackMessageDecoder.getInstance()) // since 3.x
                .register(ResetMessageDecoder.getInstance()) // since 3.x
                .register(GoodbyeMessageDecoder.getInstance()) // since 3.x
                .build();
    }

    @Override
    public final StructRegistry<Connection, RequestMessage> requestMessageRegistry() {
        return this.requestMessageStructRegistry;
    }

    protected StructRegistry<Connection, ResponseMessage> createResponseMessageRegistry() {
        return StructRegistry.<Connection, ResponseMessage>builder()
                .register(FailureMessageEncoder.getInstance())
                .register(IgnoredMessageEncoder.getInstance())
                .register(SuccessMessageEncoder.getInstance())
                .build();
    }

    @Override
    public final StructRegistry<Connection, ResponseMessage> responseMessageRegistry() {
        return this.responseMessageStructRegistry;
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
        BoltProtocol.super.registerStructWriters(pipeline);

        pipeline.addFirst(LegacyStructWriter.getInstance());
    }

    @Override
    public MetadataHandler metadataHandler() {
        return LegacyMetadataHandler.getInstance();
    }

    @Override
    public String toString() {
        return this.version().toString();
    }
}
