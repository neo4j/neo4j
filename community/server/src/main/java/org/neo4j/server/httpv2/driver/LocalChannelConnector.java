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
package org.neo4j.server.httpv2.driver;

import static org.neo4j.server.httpv2.driver.LocalChannelDriverFactory.IGNORED_HTTP_DRIVER_URI;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.local.LocalAddress;
import java.time.Clock;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelConnectedListener;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.driver.internal.async.connection.HandshakeCompletedListener;
import org.neo4j.driver.internal.async.connection.NettyChannelInitializer;
import org.neo4j.driver.internal.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.security.SecurityPlanImpl;

/**
 * A {@link ChannelConnector} which enables the driver to connect to bolt server's
 * {@link io.netty.channel.local.LocalChannel} interface.
 */
class LocalChannelConnector implements ChannelConnector {

    private static final BoltServerAddress IGNORED_ADDRESS = new BoltServerAddress(IGNORED_HTTP_DRIVER_URI);
    private final LocalAddress localAddress;
    private final Clock clock;
    private final Logging logging;
    private final String userAgent;
    private final BoltAgent boltAgent;

    private final NotificationConfig notificationConfig;
    private final AuthTokenManager authTokenManager;

    public LocalChannelConnector(
            LocalAddress localAddress,
            String userAgent,
            BoltAgent boltAgent,
            AuthTokenManager authTokenManager,
            NotificationConfig notificationConfig,
            Clock clock,
            Logging logging) {
        this.localAddress = localAddress;
        this.userAgent = userAgent;
        this.boltAgent = boltAgent;
        this.notificationConfig = notificationConfig;
        this.authTokenManager = authTokenManager;
        this.clock = clock;
        this.logging = logging;
    }

    @Override
    public ChannelFuture connect(BoltServerAddress ignored, Bootstrap bootstrap) {
        // todo address needed for tracking channels, disable tracking?
        bootstrap.handler(new NettyChannelInitializer(
                IGNORED_ADDRESS, SecurityPlanImpl.insecure(), 1000, authTokenManager, clock, logging));

        ChannelFuture channelConnected = bootstrap.connect(localAddress);

        Channel channel = channelConnected.channel();
        ChannelPromise handshakeCompleted = channel.newPromise();
        ChannelPromise connectionInitialized = channel.newPromise();

        installChannelConnectedListeners(channelConnected, handshakeCompleted);
        installHandshakeCompletedListeners(handshakeCompleted, connectionInitialized);

        return connectionInitialized;
    }

    private void installChannelConnectedListeners(ChannelFuture channelConnected, ChannelPromise handshakeCompleted) {
        var pipeline = channelConnected.channel().pipeline();

        // add timeout handler to the pipeline when channel is connected. it's needed to limit amount of time code
        // spends in TLS and Bolt handshakes. prevents infinite waiting when database does not respond
        channelConnected.addListener(future -> pipeline.addFirst(new ConnectTimeoutHandler(1000)));

        // add listener that sends Bolt handshake bytes when channel is connected
        channelConnected.addListener(new ChannelConnectedListener(
                IGNORED_ADDRESS, new ChannelPipelineBuilderImpl(), handshakeCompleted, logging));
    }

    private void installHandshakeCompletedListeners(
            ChannelPromise handshakeCompleted, ChannelPromise connectionInitialized) {
        var pipeline = handshakeCompleted.channel().pipeline();

        // remove timeout handler from the pipeline once TLS and Bolt handshakes are completed. regular protocol
        // messages will flow next and we do not want to have read timeout for them
        handshakeCompleted.addListener(future -> pipeline.remove(ConnectTimeoutHandler.class));

        // add listener that sends an INIT message. connection is now fully established. channel pipeline if fully
        // set to send/receive messages for a selected protocol version
        handshakeCompleted.addListener(new HandshakeCompletedListener(
                userAgent, boltAgent, RoutingContext.EMPTY, connectionInitialized, notificationConfig, clock));
    }
}
