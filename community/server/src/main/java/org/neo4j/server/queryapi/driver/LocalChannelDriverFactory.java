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

import io.netty.channel.local.LocalAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.DriverFactory;
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
    protected LocalAddress localAddress() {
        return localAddress;
    }

    @Override
    protected BoltConnectionProvider createBoltConnectionProvider(
            ScheduledExecutorService eventLoopGroup,
            Clock clock,
            LoggingProvider loggingProvider,
            int eventLoopThreads) {
        return new BoltConnectionProviderWithRoutingContext(
                super.createBoltConnectionProvider(eventLoopGroup, clock, loggingProvider, eventLoopThreads));
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

    /**
     * A delegating {@link BoltConnectionProvider} responsible for ensuring that 'neo4j' scheme is used, this makes sure
     * that routing context is used.
     * @param delegate the {@link BoltConnectionProvider} that it delegates to
     */
    private record BoltConnectionProviderWithRoutingContext(BoltConnectionProvider delegate)
            implements BoltConnectionProvider {
        @Override
        public CompletionStage<BoltConnection> connect(
                URI uri,
                String routingContextAddress,
                BoltAgent boltAgent,
                String userAgent,
                int connectTimeoutMillis,
                SecurityPlan securityPlan,
                AuthToken authToken,
                BoltProtocolVersion minVersion,
                NotificationConfig notificationConfig) {
            try {
                uri = new URI(
                        "neo4j",
                        uri.getUserInfo(),
                        uri.getHost(),
                        uri.getPort(),
                        uri.getPath(),
                        uri.getQuery(),
                        uri.getFragment());
            } catch (URISyntaxException e) {
                return CompletableFuture.failedStage(e);
            }
            return delegate.connect(
                    uri,
                    routingContextAddress,
                    boltAgent,
                    userAgent,
                    connectTimeoutMillis,
                    securityPlan,
                    authToken,
                    minVersion,
                    notificationConfig);
        }

        @Override
        public CompletionStage<Void> close() {
            return delegate.close();
        }
    }
}
