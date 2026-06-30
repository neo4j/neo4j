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

import static org.neo4j.util.Preconditions.checkArgument;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.thread.ThreadAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.NoopTrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.config.DomainSocketConnectorConfiguration;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.connector.error.IllegalDatabaseAccessException;
import org.neo4j.bolt.protocol.common.connector.listener.ConnectorListener;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.boltmessages.AccessMode;
import org.neo4j.boltmessages.TransactionType;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.PortBindException;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Provides a connector which provides its services through a domain socket.
 * <p/>
 * This implementation is currently limited for internal use only and is enabled via an unsupported
 * switch.
 * <p/>
 * Note that protocol level authentication will be unavailable for domain socket based
 * communication. All authorization is provided through the socket file itself and will thus occur
 * on OS level.
 */
public class DomainSocketNettyConnector extends AbstractNettyConnector<DomainSocketConnectorConfiguration> {

    private final Path path;

    public DomainSocketNettyConnector(
            String id,
            Path path,
            MemoryPool memoryPool,
            Clock clock,
            ByteBufAllocator allocator,
            EventLoopGroup bossGroup,
            EventLoopGroup workerGroup,
            ConnectorTransport transport,
            Connection.Factory connectionFactory,
            NetworkConnectionTracker connectionTracker,
            BoltProtocolRegistry protocolRegistry,
            Authentication authentication,
            AuthConfigProvider authConfigProvider,
            DefaultDatabaseResolver defaultDatabaseResolver,
            ConnectionHintRegistry connectionHintRegistry,
            TransactionManager transactionManager,
            RoutingService routingService,
            ErrorAccountant errorAccountant,
            ThreadAccountant threadAccountant,
            BoltDriverMetricsMonitor driverMetricsMonitor,
            DomainSocketConnectorConfiguration configuration,
            InternalLogProvider userLogProvider,
            InternalLogProvider logging) {
        super(
                id,
                new DomainSocketAddress(path.toFile()),
                memoryPool,
                clock,
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
                connectionHintRegistry,
                transactionManager,
                routingService,
                errorAccountant,
                NoopTrafficAccountant.getInstance(),
                threadAccountant,
                driverMetricsMonitor,
                configuration,
                userLogProvider,
                logging);
        checkArgument(
                transport.serverDomainSocketChannelType() != null,
                "Unsupported transport: " + transport.getName() + " does not support domain sockets");

        this.path = path;

        // access to user databases is restricted within the publicly available version of the
        // connector as this connector is explicitly designed for maintenance purposes - lack of
        // proper routing support also means that queries on non-leader instances would likely fail
        if (!configuration.permitUserDatabaseAccess()) {
            this.registerListener(new DomainSocketRestrictionListener());
        }
    }

    @Override
    protected Class<? extends ServerChannel> channelType() {
        return transport.serverDomainSocketChannelType();
    }

    @Override
    protected void onStart() throws Exception {
        super.onStart();

        if (Files.exists(path)) {
            if (!this.configuration().deleteSocketFile()) {
                throw new PortBindException(
                        bindAddress, new BindException("Domain socket listen file: " + path + " already exists."));
            }

            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                throw new PortBindException(bindAddress, ex);
            }
        }
    }

    @Override
    protected void onChannelBound(Channel channel) throws PortBindException {
        try {
            Files.setPosixFilePermissions(path, configuration().socketFilePermissionMask());
        } catch (IOException ex) {
            throw new PortBindException(bindAddress, ex);
        }
    }

    @Override
    protected void logStartupMessage() {
        userLog.info("Bolt (Unix Domain Socket) enabled on file %s", path);
    }

    private class DomainSocketRestrictionListener implements ConnectorListener, ConnectionListener {

        @Override
        public void onConnectionCreated(Connection connection) {
            connection.registerListener(this);
        }

        @Override
        public void onListenerAdded() {
            // NOP - make the compiler happy
        }

        @Override
        public void onListenerRemoved() {
            // NOP - make the compiler happy
        }

        @Override
        public void onPrepareTransaction(TransactionType type, AccessMode mode, String db) {
            // explicitly prevent access to databases other than "system" for now since we want to
            // avoid a situation in which queries unexpectedly get routed or access stale data
            // within a recovery scenario - this restriction may be lifted in the future
            if (!GraphDatabaseSettings.SYSTEM_DATABASE_NAME.equals(db)) {
                throw new IllegalDatabaseAccessException(
                        db,
                        "Cannot access database \"" + db
                                + "\": Only system database access is permitted via UNIX domain sockets");
            }
        }
    }

    @Override
    public boolean localQueryExecutionOnly() {
        return true;
    }
}
