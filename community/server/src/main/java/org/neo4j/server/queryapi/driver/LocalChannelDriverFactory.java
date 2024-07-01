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
package org.neo4j.server.queryapi.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import java.net.URI;
import java.time.Clock;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.GqlNotificationConfig;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.EventLoopGroupFactory;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.logging.InternalLogProvider;

/**
 * A custom {@link DriverFactory} that uses netty's {@link io.netty.channel.local.LocalChannel} to connect to the
 * bolt server.
 */
public final class LocalChannelDriverFactory extends DriverFactory {

    public static final URI IGNORED_HTTP_DRIVER_URI = URI.create("bolt://http-driver.com:0");
    private final LocalAddress localAddress;
    private final InternalLogProvider internalLogProvider;

    public LocalChannelDriverFactory(LocalAddress localAddress, InternalLogProvider internalLogProvider) {
        this.localAddress = localAddress;
        this.internalLogProvider = internalLogProvider;
    }

    @Override
    protected Bootstrap createBootstrap(int threadCount) {
        return newBootstrap(threadCount);
    }

    @Override
    protected ChannelConnector createConnector(
            ConnectionSettings settings,
            SecurityPlan securityPlan,
            Config config,
            Clock clock,
            RoutingContext routingContext,
            BoltAgent boltAgent) {
        return new LocalChannelConnector(
                localAddress,
                config.userAgent(),
                boltAgent,
                settings.authTokenProvider(),
                GqlNotificationConfig.from(config.notificationConfig()),
                securityPlan,
                clock,
                config.logging());
    }

    public Driver createLocalDriver() {
        return super.newInstance(
                IGNORED_HTTP_DRIVER_URI,
                new StaticAuthTokenManager(AuthTokens.none()),
                null,
                Config.builder()
                        .withLogging(new DriverToInternalLogProvider(internalLogProvider))
                        .withUserAgent("neo4j-query-api/v2")
                        .build());
    }

    public static Bootstrap newBootstrap(int threadCount) {
        var bootstrap = new Bootstrap();
        bootstrap.group(EventLoopGroupFactory.newEventLoopGroup(threadCount));
        bootstrap.channel(LocalChannel.class);
        return bootstrap;
    }
}
