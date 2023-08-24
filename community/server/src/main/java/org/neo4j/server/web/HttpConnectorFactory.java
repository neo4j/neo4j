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

import java.util.Arrays;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.server.configuration.ServerSettings;

public class HttpConnectorFactory {
    private static final String NAME = "http";

    private final String name;
    private final NetworkConnectionTracker connectionTracker;
    private final Config configuration;
    private final ByteBufferPool byteBufferPool;

    public HttpConnectorFactory(
            NetworkConnectionTracker connectionTracker, Config config, ByteBufferPool byteBufferPool) {
        this(NAME, connectionTracker, config, byteBufferPool);
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

    public ConnectionFactory createHttpConnectionFactory(boolean requiresHostnameVerification) {
        return new JettyHttpConnectionFactory(connectionTracker, createHttpConfig(requiresHostnameVerification));
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
        ConnectionFactory httpFactory = createHttpConnectionFactory(false);
        return createConnector(server, address, jettyThreadCalculator, httpFactory);
    }

    public ServerConnector createConnector(
            Server server,
            SocketAddress address,
            JettyThreadCalculator jettyThreadCalculator,
            ConnectionFactory... httpFactories) {
        int acceptors = jettyThreadCalculator.getAcceptors();
        int selectors = jettyThreadCalculator.getSelectors();

        ServerConnector connector =
                new ServerConnector(server, null, null, byteBufferPool, acceptors, selectors, httpFactories);

        connector.setName(name);

        connector.setConnectionFactories(Arrays.asList(httpFactories));

        // TCP backlog, per socket, 50 is the default, consider adapting if needed
        connector.setAcceptQueueSize(50);

        connector.setHost(address.getHostname());
        connector.setPort(address.getPort());

        return connector;
    }
}
