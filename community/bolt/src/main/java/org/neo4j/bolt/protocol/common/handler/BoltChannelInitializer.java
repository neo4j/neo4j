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
package org.neo4j.bolt.protocol.common.handler;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.pcap.PcapWriteHandler;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;

/**
 * Initializes generic netty pipelines for use with the Bolt protocol.
 */
public class BoltChannelInitializer extends ChannelInitializer<Channel> {
    private static final long SHALLOW_SIZE_PACKET_CAPTURE = HeapEstimator.shallowSizeOfInstance(PcapWriteHandler.class);

    private final Config config;
    private final Connector connector;
    private final ByteBufAllocator allocator;
    private final SslContext sslContext;
    private final InternalLogProvider logging;
    private final InternalLog log;

    public BoltChannelInitializer(
            Config config,
            Connector connector,
            ByteBufAllocator allocator,
            SslContext sslContext,
            InternalLogProvider logging) {
        this.config = config;
        this.allocator = allocator;
        this.connector = connector;
        this.sslContext = sslContext;
        this.logging = logging;

        this.log = logging.getLog(BoltChannelInitializer.class);
    }

    public BoltChannelInitializer(
            Config config, Connector connector, ByteBufAllocator allocator, InternalLogProvider logging) {
        this(config, connector, allocator, null, logging);
    }

    @Override
    protected void initChannel(Channel ch) {
        log.debug("Incoming connection from %s", ch.remoteAddress());

        // ensure that this newly created channel makes use of the designated buffer allocator for its receive and send
        // buffers respectively
        ch.config().setAllocator(this.allocator);

        // acquire a new connection from our connector and register it for use with the network channel
        var connection = this.connector.createConnection(ch);

        // continue by initializing a network pipeline for the purposes of encapsulation and protocol
        // negotiation - once complete the channel is ready to negotiate a protocol revision
        connection
                .memoryTracker()
                .allocateHeap(HeapEstimator.sizeOf(ch)
                        + TransportSelectionHandler.SHALLOW_SIZE
                        + TrafficAccountantHandler.SHALLOW_SIZE);

        ch.pipeline().addLast(new TrafficAccountantHandler(this.connector.trafficAccountant()));

        // when enabled, also register a protocol capture handler which writes all network
        // communication for this channel into a dedicated file
        if (this.config.get(BoltConnectorInternalSettings.protocol_capture)) {
            connection.memoryTracker().allocateHeap(SHALLOW_SIZE_PACKET_CAPTURE);

            var file = this.config
                    .get(BoltConnectorInternalSettings.protocol_capture_path)
                    .resolve(connection.id() + ".pcap")
                    .toAbsolutePath();

            try {
                Files.createDirectories(file.getParent());

                ch.pipeline()
                        .addLast(
                                "captureHandler",
                                PcapWriteHandler.builder()
                                        .build(Files.newOutputStream(
                                                file,
                                                StandardOpenOption.CREATE,
                                                StandardOpenOption.TRUNCATE_EXISTING)));

                log.info(
                        "[%s] Created packet capture for connection %s at %s",
                        ch.remoteAddress(), connection.id(), file);
            } catch (IOException ex) {
                log.warn("[" + ch.remoteAddress() + "] Failed to initialize Bolt capture handler for connection", ex);
            }
        }

        // when explicitly enabled, also register a protocol logging handler within the pipeline in order to
        // print all incoming and outgoing traffic to the internal log - this has performance implications thus
        // requiring its own dedicated configuration option
        var enableLogging = this.config.get(BoltConnectorInternalSettings.protocol_logging);
        var loggingMode = this.config.get(BoltConnectorInternalSettings.protocol_logging_mode);

        ch.pipeline()
                .addLast(new TransportSelectionHandler(
                        this.config, this.sslContext, enableLogging, loggingMode, this.logging));

        connection.notifyListeners(listener -> listener.onNetworkPipelineInitialized(ch.pipeline()));
    }
}
