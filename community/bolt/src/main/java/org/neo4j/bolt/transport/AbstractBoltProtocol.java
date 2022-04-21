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
package org.neo4j.bolt.transport;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.packstream.ChunkedOutput;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.packstream.PackOutput;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.transport.pipeline.ChannelProtector;
import org.neo4j.bolt.transport.pipeline.ChunkDecoder;
import org.neo4j.bolt.transport.pipeline.HouseKeeper;
import org.neo4j.bolt.transport.pipeline.MessageAccumulator;
import org.neo4j.bolt.transport.pipeline.MessageDecoder;
import org.neo4j.bolt.v3.runtime.bookmarking.BookmarksParserV3;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * The base of building Bolt protocols.
 */
public abstract class AbstractBoltProtocol implements BoltProtocol {
    private final BoltChannel channel;
    private final Config config;
    private final LogService logging;
    private final TransportThrottleGroup throttleGroup;
    private final ChannelProtector channelProtector;
    private final MemoryTracker memoryTracker;

    private final BoltStateMachineFactory stateMachineFactory;
    private final BoltConnectionFactory connectionFactory;
    private final BookmarksParser bookmarksParser;
    private final MapValue connectionHints;

    public AbstractBoltProtocol(
            BoltChannel channel,
            BoltConnectionFactory connectionFactory,
            BoltStateMachineFactory stateMachineFactory,
            Config config,
            LogService logging,
            TransportThrottleGroup throttleGroup,
            ChannelProtector channelProtector,
            MemoryTracker memoryTracker) {
        this(
                channel,
                connectionFactory,
                stateMachineFactory,
                config,
                BookmarksParserV3.INSTANCE,
                logging,
                throttleGroup,
                channelProtector,
                memoryTracker);
    }

    protected AbstractBoltProtocol(
            BoltChannel channel,
            BoltConnectionFactory connectionFactory,
            BoltStateMachineFactory stateMachineFactory,
            Config config,
            BookmarksParser bookmarksParser,
            LogService logging,
            TransportThrottleGroup throttleGroup,
            ChannelProtector channelProtector,
            MemoryTracker memoryTracker) {
        this.channel = channel;
        this.config = config;
        this.logging = logging;
        this.throttleGroup = throttleGroup;
        this.stateMachineFactory = stateMachineFactory;
        this.connectionFactory = connectionFactory;
        this.bookmarksParser = bookmarksParser;
        this.channelProtector = channelProtector;
        this.memoryTracker = memoryTracker;

        var hintBuilder = new MapValueBuilder(1);
        if (config.get(BoltConnector.connection_keep_alive_type) == BoltConnector.KeepAliveRequestType.ALL) {
            var keepAliveInterval = config.get(BoltConnector.connection_keep_alive);
            var keepAliveProbes = config.get(BoltConnector.connection_keep_alive_probes);

            hintBuilder.add(
                    "connection.recv_timeout_seconds",
                    Values.longValue(keepAliveInterval.toSeconds() * keepAliveProbes));
        }
        this.connectionHints = hintBuilder.build();
    }

    /**
     * Install chunker, packstream, message reader, message handler, message encoder for protocol v1
     */
    @Override
    public void install() {
        BoltStateMachine stateMachine =
                stateMachineFactory.newStateMachine(version(), channel, connectionHints, memoryTracker);
        var neo4jPack = createPack(memoryTracker);
        var messageWriter = createMessageWriter(neo4jPack, logging, memoryTracker);

        var connection = connectionFactory.newConnection(channel, stateMachine, messageWriter);
        var messageReader = createMessageReader(
                connection, messageWriter, bookmarksParser, logging, channelProtector, memoryTracker);

        memoryTracker.allocateHeap(ChunkDecoder.SHALLOW_SIZE
                + MessageAccumulator.SHALLOW_SIZE
                + MessageDecoder.SHALLOW_SIZE
                + HouseKeeper.SHALLOW_SIZE);

        channel.installBoltProtocol(
                new ChunkDecoder(),
                new MessageAccumulator(config),
                new MessageDecoder(neo4jPack, messageReader, logging),
                new HouseKeeper(connection, logging.getInternalLog(HouseKeeper.class)));
    }

    protected PackOutput createPackOutput(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(ChunkedOutput.SHALLOW_SIZE);
        return new ChunkedOutput(channel.rawChannel(), throttleGroup);
    }

    /**
     * visible for testing
     **/
    public Neo4jPack createPack(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(Neo4jPackV2.SHALLOW_SIZE);
        return new Neo4jPackV2();
    }

    protected abstract BoltRequestMessageReader createMessageReader(
            BoltConnection connection,
            BoltResponseMessageWriter messageWriter,
            BookmarksParser bookmarksParser,
            LogService logging,
            ChannelProtector channelProtector,
            MemoryTracker memoryTracker);

    protected abstract BoltResponseMessageWriter createMessageWriter(
            Neo4jPack neo4jPack, LogService logging, MemoryTracker memoryTracker);
}
