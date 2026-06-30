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

import static org.mockito.ArgumentMatchers.any;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.pcap.PcapWriteHandler;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connector.config.NettyConnectorConfiguration;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.bolt.testing.mock.ConnectorMockFactory;
import org.neo4j.bolt.testing.mock.TestConnectorConfiguration;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

class BoltChannelInitializerTest {

    private Config config;
    private AbstractNettyConnector connector;
    private Connection connection;
    private ByteBufAllocator allocator;
    private AssertableLogProvider logProvider;

    private BoltChannelInitializer initializer;

    @BeforeEach
    void prepareDependencies() {
        this.config = Mockito.spy(Config.defaults());
        this.connection = ConnectionMockFactory.newInstance();
        this.connector = ConnectorMockFactory.newFactory()
                .withConnection(this.connection)
                .build();
        this.allocator = Mockito.mock(ByteBufAllocator.class);
        this.logProvider = new AssertableLogProvider();

        this.initializer = new BoltChannelInitializer(this.connector, this.allocator, this.logProvider);
    }

    @Test
    void shouldAllocateConnection() {
        var channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);

        this.initializer.initChannel(channel);

        Mockito.verify(this.connector).createConnection(channel);
    }

    @Test
    void shouldInstallHandlers() {
        var memoryTracker = Mockito.mock(MemoryTracker.class);
        var pipeline = Mockito.mock(ChannelPipeline.class, Mockito.RETURNS_SELF);
        var channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);

        Mockito.doReturn(pipeline).when(channel).pipeline();
        Mockito.doReturn(memoryTracker).when(this.connection).memoryTracker();

        this.initializer.initChannel(channel);

        var inOrder = Mockito.inOrder(memoryTracker, pipeline);

        inOrder.verify(memoryTracker)
                .allocateHeap(HeapEstimator.sizeOf(channel)
                        + TransportSelectionHandler.SHALLOW_SIZE
                        + TrafficAccountantHandler.SHALLOW_SIZE
                        + DeadlockReportingHandler.SHALLOW_SIZE);

        inOrder.verify(pipeline).addLast(any(TrafficAccountantHandler.class));
        inOrder.verify(pipeline).addLast(any(TransportSelectionHandler.class));
        inOrder.verifyNoMoreInteractions();
        verifyProtocolNotSelected(pipeline);
    }

    @Test
    void shouldNotifyListeners() {
        var listener = Mockito.mock(ConnectionListener.class);
        var connection = ConnectionMockFactory.newInstance();
        var pipeline = Mockito.mock(ChannelPipeline.class, Mockito.RETURNS_SELF);
        var channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);

        Mockito.doReturn(pipeline).when(channel).pipeline();
        Mockito.doReturn(connection).when(this.connector).createConnection(channel);

        this.initializer.initChannel(channel);

        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Consumer<ConnectionListener>> captor = (ArgumentCaptor) ArgumentCaptor.forClass(Consumer.class);

        Mockito.verify(connection).notifyListeners(captor.capture());

        var notificationFunction = captor.getValue();

        Assertions.assertThat(notificationFunction).isNotNull();

        notificationFunction.accept(listener);

        Mockito.verify(listener).onNetworkPipelineInitialized(pipeline);

        verifyProtocolNotSelected(pipeline);
    }

    // disabled on Windows as file locking causes problems with temporary file deletion
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldInstallCaptureListener() throws IOException {
        var path = Files.createTempDirectory("bolt_");

        try {
            var config = TestConnectorConfiguration.factory()
                    .enableProtocolCapture(true)
                    .protocolCapturePath(path)
                    .build();

            Mockito.doReturn(config).when(this.connector).configuration();

            var memoryTracker = Mockito.mock(MemoryTracker.class);
            var pipeline = Mockito.mock(ChannelPipeline.class, Mockito.RETURNS_SELF);
            var channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);

            Mockito.doReturn(pipeline).when(channel).pipeline();
            Mockito.doReturn(memoryTracker).when(this.connection).memoryTracker();

            this.initializer.initChannel(channel);

            var captor = ArgumentCaptor.forClass(PcapWriteHandler.class);
            Mockito.verify(pipeline).addLast(ArgumentMatchers.eq("captureHandler"), captor.capture());

            var handler = captor.getValue();

            Assertions.assertThat(handler).isNotNull();

            // ensure initialization since PcapWriteHandler fails to close the output stream if
            // added to a mock channel since it never creates PcapWriter
            try {
                handler.channelActive(Mockito.mock(ChannelHandlerContext.class, Mockito.RETURNS_MOCKS));
            } catch (Exception ignore) {
            }

            try {
                handler.close();
            } catch (Exception ignore) {
            }
        } finally {
            FileUtils.deleteDirectory(path);
        }
    }

    @Test
    void shouldSupportJavaObjectMessages() {
        var listener = Mockito.mock(ConnectionListener.class);
        var connection = ConnectionMockFactory.newInstance();
        var pipeline = Mockito.mock(ChannelPipeline.class, Mockito.RETURNS_SELF);
        var channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);
        var connectionConfiguration = Mockito.mock(NettyConnectorConfiguration.class, Mockito.RETURNS_MOCKS);
        var protocolRegistry = Mockito.mock(BoltProtocolRegistry.class, Mockito.RETURNS_MOCKS);
        var expectedProtocol = BoltProtocol.latestInstalled();

        Mockito.doReturn(pipeline).when(channel).pipeline();
        Mockito.doReturn(connection).when(this.connector).createConnection(channel);
        Mockito.doReturn(connectionConfiguration).when(connector).configuration();
        Mockito.doReturn(true).when(connectionConfiguration).enableJavaObjectMessages();
        Mockito.doReturn(protocolRegistry).when(connector).protocolRegistry();
        Mockito.doReturn(Optional.of(expectedProtocol)).when(protocolRegistry).get(any());

        this.initializer.initChannel(channel);

        Mockito.verify(connection).selectProtocol(expectedProtocol, Set.of());
        Mockito.verify(pipeline).addLast(Mockito.eq("requestHandler"), any(RequestHandler.class));

        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Consumer<ConnectionListener>> captor = (ArgumentCaptor) ArgumentCaptor.forClass(Consumer.class);
        Mockito.verify(connection, Mockito.times(2)).notifyListeners(captor.capture());
        for (var notifier : captor.getAllValues()) {
            Assertions.assertThat(notifier).isNotNull();
            notifier.accept(listener);
        }

        Mockito.verify(listener).onNetworkPipelineInitialized(pipeline);
        Mockito.verify(listener).onProtocolSelected(expectedProtocol);
    }

    private void verifyProtocolNotSelected(ChannelPipeline pipeline) {
        Mockito.verify(connection, Mockito.never()).selectProtocol(any(), any());
        Mockito.verify(pipeline, Mockito.never()).addLast(Mockito.eq("requestHandler"), any(RequestHandler.class));
    }

    @Test
    void shouldNotInstallSeparateProxyProtocolDetector() {
        // PROXY protocol detection is now integrated into TransportSelectionHandler
        // to avoid conflicts with byte accumulation requirements
        var config =
                TestConnectorConfiguration.factory().enableProxyProtocol(true).build();

        Mockito.doReturn(config).when(this.connector).configuration();

        var memoryTracker = Mockito.mock(MemoryTracker.class);
        var pipeline = Mockito.mock(ChannelPipeline.class, Mockito.RETURNS_SELF);
        var channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);

        Mockito.doReturn(pipeline).when(channel).pipeline();
        Mockito.doReturn(memoryTracker).when(this.connection).memoryTracker();

        this.initializer.initChannel(channel);

        // Verify that no separate PROXY protocol detector handler is added
        // PROXY protocol detection happens dynamically in TransportSelectionHandler
        Mockito.verify(pipeline, Mockito.never()).addLast(Mockito.eq("haproxyDetector"), any());
    }
}
