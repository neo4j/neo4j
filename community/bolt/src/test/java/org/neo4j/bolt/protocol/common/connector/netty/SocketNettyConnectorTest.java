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
package org.neo4j.bolt.protocol.common.connector.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.time.Clock;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.TrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.protocol.common.connector.transport.NioConnectorTransport;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.helpers.PortBindException;
import org.neo4j.dbms.routing.RoutingService;

class SocketNettyConnectorTest extends AbstractNettyConnectorTest<SocketNettyConnector> {

    private static final String CONNECTOR_ID = "bolt-socket-test";

    private Config config;
    private ConnectorPortRegister connectorPortRegister;
    private ByteBufAllocator allocator;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ConnectorTransport transport;

    @Override
    @BeforeEach
    protected void prepareDependencies() {
        super.prepareDependencies();

        config = Mockito.spy(Config.defaults());
        connectorPortRegister = Mockito.mock(ConnectorPortRegister.class);
        allocator = ByteBufAllocator.DEFAULT;
        transport = new NioConnectorTransport();
        bossGroup = transport.createEventLoopGroup(new DefaultThreadFactory("bolt-network"));
        workerGroup = bossGroup; // currently shared in production code
    }

    @AfterEach
    void cleanupDependencies() {
        bossGroup.shutdownNow();
    }

    protected SocketNettyConnector createConnector(SocketAddress bindAddress) {
        return new SocketNettyConnector(
                CONNECTOR_ID,
                bindAddress,
                config,
                ConnectorType.BOLT,
                connectorPortRegister,
                memoryPool,
                Clock.systemUTC(),
                allocator,
                bossGroup,
                workerGroup,
                transport,
                connectionFactory,
                connectionTracker,
                null,
                false,
                true,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintRegistry,
                Mockito.mock(TransactionManager.class),
                Mockito.mock(RoutingService.class),
                Mockito.mock(ErrorAccountant.class),
                Mockito.mock(TrafficAccountant.class),
                BoltDriverMetricsMonitor.noop(),
                512,
                0,
                logging,
                logging);
    }

    @Override
    protected SocketAddress getDefaultAddress() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    @Test
    void shouldBindToSpecifiedAddress() throws Exception {
        connector = createConnector();

        Assertions.assertThat(connector.address()).isNull();

        connector.start();

        var address = connector.address();

        // ensure that the server binds to the address we told it to bind to with a randomly assigned port
        Assertions.assertThat(address)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(InetSocketAddress.class))
                .extracting(InetSocketAddress::getAddress)
                .isEqualTo(InetAddress.getLoopbackAddress());

        Assertions.assertThat(address)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(InetSocketAddress.class))
                .extracting(InetSocketAddress::getPort)
                .isNotEqualTo(0);

        try (var connection = new Socket()) {
            connection.connect(connector.address());
        }
    }

    @Test
    void shouldRegisterWithPortRegister() throws Exception {
        connector = createConnector();
        connector.start();

        var address = connector.address();

        Assertions.assertThat(address).isNotNull().isInstanceOf(InetSocketAddress.class);

        Mockito.verify(connectorPortRegister).register(ConnectorType.BOLT, (InetSocketAddress) address);
    }

    @Test
    void shouldFailWithPortBindErrorWhenPortConflicts() throws IOException {
        try (var channel = ServerSocketChannel.open().bind(getDefaultAddress())) {
            var bindAddress = channel.getLocalAddress();

            connector = createConnector(bindAddress);

            Assertions.assertThatExceptionOfType(PortBindException.class)
                    .isThrownBy(() -> connector.start())
                    .withRootCauseInstanceOf(BindException.class);
        }
    }
}
