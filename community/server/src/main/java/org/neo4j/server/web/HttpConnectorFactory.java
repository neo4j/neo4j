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
package org.neo4j.server.web;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.server.configuration.ConfigurableTransports;
import org.neo4j.server.configuration.ServerSettings;

public class HttpConnectorFactory {
    private static final String HTTP_NAME = "http";
    private static final String HTTP2_NAME = "http2";

    private final String name;
    protected final NetworkConnectionTracker connectionTracker;
    protected final Config configuration;
    private final ByteBufferPool byteBufferPool;

    public HttpConnectorFactory(
            NetworkConnectionTracker connectionTracker, Config config, ByteBufferPool byteBufferPool) {
        this(HTTP_NAME, connectionTracker, config, byteBufferPool);
    }

    protected HttpConnectorFactory(
            String name,
            NetworkConnectionTracker connectionTracker,
            Config configuration,
            ByteBufferPool byteBufferPool) {
        this.name = name;
        this.connectionTracker = connectionTracker;
        this.configuration = configuration;
        this.byteBufferPool = byteBufferPool;
    }

    public List<ConnectionFactory> createHttpConnectionFactories(boolean requiresHostnameVerification) {
        var httpConfig = createHttpConfig(requiresHostnameVerification);
        var connectionFactories = new ArrayList<ConnectionFactory>();

        if (configuration.get(ServerSettings.http_enabled_transports).contains(ConfigurableTransports.HTTP1_1)) {
            connectionFactories.add(new JettyHttpConnectionFactory(connectionTracker, httpConfig));
        }

        if (configuration.get(ServerSettings.http_enabled_transports).contains(ConfigurableTransports.HTTP2)) {
            var http2ConnectionListener = new JettyHttp2ConnectionListener(connectionTracker, HTTP2_NAME);
            var h2c = new HTTP2CServerConnectionFactory(httpConfig);
            h2c.addBean(http2ConnectionListener);
            var directHttp2 = new HTTP2ServerConnectionFactory(httpConfig);
            directHttp2.addBean(http2ConnectionListener);
            connectionFactories.add(h2c);
            connectionFactories.add(directHttp2);
        }

        return connectionFactories;
    }

    protected HttpConfiguration createHttpConfig(boolean ignored) {
        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setRequestHeaderSize(configuration.get(ServerSettings.maximum_request_header_size));
        httpConfig.setResponseHeaderSize(configuration.get(ServerSettings.maximum_response_header_size));
        httpConfig.setSendServerVersion(false);
        return httpConfig;
    }

    public ServerConnector createConnector(
            Server server, SocketAddress address, JettyThreadCalculator jettyThreadCalculator) {
        List<ConnectionFactory> httpFactories = createHttpConnectionFactories(false);
        return createConnector(server, address, jettyThreadCalculator, httpFactories);
    }

    public ServerConnector createConnector(
            Server server,
            SocketAddress address,
            JettyThreadCalculator jettyThreadCalculator,
            List<ConnectionFactory> httpFactories) {
        int acceptors = jettyThreadCalculator.getAcceptors();
        int selectors = jettyThreadCalculator.getSelectors();

        ServerConnector connector = new ServerConnector(
                server,
                null,
                null,
                byteBufferPool,
                acceptors,
                selectors,
                httpFactories.toArray(new ConnectionFactory[0]));

        connector.setName(name);

        connector.setConnectionFactories(httpFactories);

        // TCP backlog, per socket, 50 is the default, consider adapting if needed
        connector.setAcceptQueueSize(50);

        connector.setHost(address.getHostname());
        connector.setPort(address.getPort());

        return connector;
    }
}
