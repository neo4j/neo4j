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
package org.neo4j.bolt.protocol.common.connector.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.protocol.common.connector.transport.EpollConnectorTransport;
import org.neo4j.bolt.protocol.common.connector.transport.KqueueConnectorTransport;
import org.neo4j.bolt.protocol.common.connector.transport.NioConnectorTransport;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.PortBindException;

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

        this.config = Mockito.spy(Config.defaults());
        this.allocator = ByteBufAllocator.DEFAULT;

        // currently NIO does not provide support for domain sockets (despite JDK support) thus limiting this
        // functionality to compatible versions of Linux based operating systems as well as certain versions of Mac OS
        var epollTransport = new EpollConnectorTransport();
        if (epollTransport.isAvailable()) {
            this.transport = epollTransport;
        } else {
            var kqueueTransport = new KqueueConnectorTransport();
            if (kqueueTransport.isAvailable()) {
                this.transport = kqueueTransport;
            }
        }

        Assumptions.assumeTrue(this.transport != null);

        this.bossGroup = this.transport.createEventLoopGroup(new DefaultThreadFactory("bolt-network"));
        this.workerGroup = this.bossGroup; // currently shared in production code
    }

    @BeforeEach
    void deleteDomainSocket() throws IOException {
        // ensure that no files are left over from a previously crashed JVM instance
        Files.deleteIfExists(Paths.get(FILE_NAME));
    }

    @Override
    protected DomainSocketNettyConnector createConnector(SocketAddress address) {
        return new DomainSocketNettyConnector(
                CONNECTOR_ID,
                new File(((DomainSocketAddress) address).path()),
                this.config,
                this.memoryPool,
                this.allocator,
                this.bossGroup,
                this.workerGroup,
                this.transport,
                this.connectionFactory,
                this.connectionTracker,
                this.protocolRegistry,
                this.authentication,
                this.authConfigProvider,
                this.defaultDatabaseResolver,
                this.connectionHintProvider,
                this.logging,
                this.logging);
    }

    @Override
    protected SocketAddress getDefaultAddress() {
        return new DomainSocketAddress(FILE_NAME);
    }

    @Test
    void shouldBindSpecifiedAddress() throws Exception {
        this.connector = this.createConnector();

        Assertions.assertThat(this.connector.address()).isNull();

        this.connector.start();

        var address = this.connector.address();

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

        this.connector.stop();
        this.connector = null;

        Assertions.assertThat(path).doesNotExist();
    }

    @Test
    void shouldFailWithIllegalArgumentWhenTransportIsIncompatible() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new DomainSocketNettyConnector(
                        CONNECTOR_ID,
                        new File(FILE_NAME),
                        this.config,
                        this.memoryPool,
                        this.allocator,
                        this.bossGroup,
                        this.workerGroup,
                        new NioConnectorTransport(),
                        this.connectionFactory,
                        this.connectionTracker,
                        this.protocolRegistry,
                        this.authentication,
                        this.authConfigProvider,
                        this.defaultDatabaseResolver,
                        this.connectionHintProvider,
                        this.logging,
                        this.logging))
                .withMessageContaining("Unsupported transport: NIO does not support domain sockets")
                .withNoCause();
    }

    @Test
    void shouldFailWithPortBindErrorWhenPortConflicts() {
        var bindAddress = this.getDefaultAddress();

        try (var ignored = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
                .bind(UnixDomainSocketAddress.of(((DomainSocketAddress) bindAddress).path()))) {
            this.connector = this.createConnector(bindAddress);

            Assertions.assertThatExceptionOfType(PortBindException.class)
                    .isThrownBy(() -> this.connector.start())
                    .withRootCauseInstanceOf(BindException.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
