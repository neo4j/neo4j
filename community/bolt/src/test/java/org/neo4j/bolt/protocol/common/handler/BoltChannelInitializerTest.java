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
package org.neo4j.bolt.protocol.common.handler;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelPipeline;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.bolt.testing.mock.ConnectorMockFactory;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

class BoltChannelInitializerTest {

    private Config config;
    private Connector connector;
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

        this.initializer = new BoltChannelInitializer(this.config, this.connector, this.allocator, this.logProvider);
    }

    @Test
    void shouldConfigureAllocator() {
        var config = Mockito.mock(ChannelConfig.class);
        var channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);

        Mockito.doReturn(config).when(channel).config();

        this.initializer.initChannel(channel);

        Mockito.verify(config).setAllocator(this.allocator);
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
                .allocateHeap(HeapEstimator.sizeOf(channel) + TransportSelectionHandler.SHALLOW_SIZE);

        inOrder.verify(pipeline).addLast(ArgumentMatchers.any(TransportSelectionHandler.class));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldInstallProtocolLoggingHandlers() {
        Mockito.doReturn(true).when(this.config).get(BoltConnectorInternalSettings.protocol_logging);

        var memoryTracker = Mockito.mock(MemoryTracker.class);
        var pipeline = Mockito.mock(ChannelPipeline.class, Mockito.RETURNS_SELF);
        var channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);

        Mockito.doReturn(pipeline).when(channel).pipeline();
        Mockito.doReturn(memoryTracker).when(this.connection).memoryTracker();

        this.initializer.initChannel(channel);

        var inOrder = Mockito.inOrder(memoryTracker, pipeline);

        inOrder.verify(memoryTracker).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);

        inOrder.verify(pipeline)
                .addLast(
                        ArgumentMatchers.eq("protocolLoggingHandler"),
                        ArgumentMatchers.any(ProtocolLoggingHandler.class));
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
    }
}
