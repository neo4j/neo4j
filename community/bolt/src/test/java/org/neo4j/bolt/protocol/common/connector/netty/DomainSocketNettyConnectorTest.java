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
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.BindException;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.protocol.common.connector.transport.EpollConnectorTransport;
import org.neo4j.bolt.protocol.common.connector.transport.KqueueConnectorTransport;
import org.neo4j.bolt.protocol.common.connector.transport.NioConnectorTransport;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.PortBindException;
import org.neo4j.dbms.routing.RoutingService;

class DomainSocketNettyConnectorTest extends AbstractNettyConnectorTest<DomainSocketNettyConnector> {

    private static final String CONNECTOR_ID = "bolt-domain-test";
    private static final String FILE_NAME = CONNECTOR_ID + ".tmp";

    private Config config;
    private ByteBufAllocator allocator;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ConnectorTransport transport;

    @Override
    @BeforeEach
    protected void prepareDependencies() {
        super.prepareDependencies();

        config = Mockito.spy(Config.defaults());
        allocator = ByteBufAllocator.DEFAULT;

        // currently NIO does not provide support for domain sockets (despite JDK support) thus limiting this
        // functionality to compatible versions of Linux based operating systems as well as certain versions of Mac OS
        var epollTransport = new EpollConnectorTransport();
        if (epollTransport.isAvailable()) {
            transport = epollTransport;
        } else {
            var kqueueTransport = new KqueueConnectorTransport();
            if (kqueueTransport.isAvailable()) {
                transport = kqueueTransport;
            }
        }

        Assumptions.assumeTrue(transport != null);

        bossGroup = transport.createEventLoopGroup(new DefaultThreadFactory("bolt-network"));
        workerGroup = bossGroup; // currently shared in production code
    }

    @BeforeEach
    void deleteDomainSocket() throws IOException {
        // ensure that no files are left over from a previously crashed JVM instance
        Files.deleteIfExists(Path.of(FILE_NAME));
    }

    @Override
    protected DomainSocketNettyConnector createConnector(SocketAddress address) {
        var config = new DomainSocketNettyConnector.DomainSocketConfiguration(
                false, null, false, null, 0, 0, 0, false, 0, 0, null, 0, 0, 512, 0, Duration.ofHours(5), false, false);

        return new DomainSocketNettyConnector(
                CONNECTOR_ID,
                Path.of(((DomainSocketAddress) address).path()),
                memoryPool,
                Clock.systemUTC(),
                allocator,
                bossGroup,
                workerGroup,
                transport,
                connectionFactory,
                connectionTracker,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                this.connectionHintRegistry,
                Mockito.mock(TransactionManager.class),
                Mockito.mock(RoutingService.class),
                Mockito.mock(ErrorAccountant.class),
                BoltDriverMetricsMonitor.noop(),
                config,
                logging,
                logging);
    }

    @Override
    protected SocketAddress getDefaultAddress() {
        return new DomainSocketAddress(FILE_NAME);
    }

    @Test
    void shouldBindSpecifiedAddress() throws Exception {
        connector = createConnector();

        Assertions.assertThat(connector.address()).isNull();

        connector.start();

        var address = connector.address();

        Assertions.assertThat(address)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(DomainSocketAddress.class))
                .extracting(DomainSocketAddress::path)
                .asString()
                .endsWith(FILE_NAME);

        var path = Path.of(((DomainSocketAddress) address).path());
        Assertions.assertThat(path).exists();

        try (var channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            channel.connect(UnixDomainSocketAddress.of(((DomainSocketAddress) address).path()));
        }

        connector.stop();
        connector = null;

        Assertions.assertThat(path).doesNotExist();
    }

    @Test
    void shouldFailWithIllegalArgumentWhenTransportIsIncompatible() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> {
                    var config = new DomainSocketNettyConnector.DomainSocketConfiguration(
                            false,
                            null,
                            false,
                            null,
                            0,
                            0,
                            0,
                            false,
                            0,
                            0,
                            null,
                            0,
                            0,
                            512,
                            0,
                            Duration.ofHours(5),
                            false,
                            true);

                    new DomainSocketNettyConnector(
                            CONNECTOR_ID,
                            Path.of(FILE_NAME),
                            memoryPool,
                            Clock.systemUTC(),
                            allocator,
                            bossGroup,
                            workerGroup,
                            new NioConnectorTransport(),
                            connectionFactory,
                            connectionTracker,
                            protocolRegistry,
                            authentication,
                            authConfigProvider,
                            defaultDatabaseResolver,
                            connectionHintRegistry,
                            Mockito.mock(TransactionManager.class),
                            Mockito.mock(RoutingService.class),
                            Mockito.mock(ErrorAccountant.class),
                            BoltDriverMetricsMonitor.noop(),
                            config,
                            logging,
                            logging);
                })
                .withMessageContaining("Unsupported transport: NIO does not support domain sockets")
                .withNoCause();
    }

    @Test
    void shouldFailWithPortBindErrorWhenPortConflicts() {
        var bindAddress = getDefaultAddress();

        try (var ignored = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
                .bind(UnixDomainSocketAddress.of(((DomainSocketAddress) bindAddress).path()))) {
            connector = createConnector(bindAddress);

            Assertions.assertThatExceptionOfType(PortBindException.class)
                    .isThrownBy(() -> connector.start())
                    .withRootCauseInstanceOf(BindException.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
