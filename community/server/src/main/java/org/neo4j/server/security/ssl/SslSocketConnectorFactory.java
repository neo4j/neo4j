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
package org.neo4j.server.security.ssl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConfiguration.Customizer;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.server.configuration.ConfigurableTransports;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.web.HttpConnectorFactory;
import org.neo4j.server.web.JettyHttp2ConnectionListener;
import org.neo4j.server.web.JettyHttpConnectionFactory;
import org.neo4j.server.web.JettyThreadCalculator;
import org.neo4j.ssl.SslPolicy;

public class SslSocketConnectorFactory extends HttpConnectorFactory {
    private static final String HTTPS_NAME = "https";
    private static final String HTTPS2_NAME = "https2";

    private final Customizer requestCustomizer;

    public SslSocketConnectorFactory(
            NetworkConnectionTracker connectionTracker, Config config, ByteBufferPool byteBufferPool) {
        super(HTTPS_NAME, connectionTracker, config, byteBufferPool);
        requestCustomizer = new HttpsRequestCustomizer(config);
    }

    @Override
    protected HttpConfiguration createHttpConfig(boolean requiresHostnameVerification) {
        HttpConfiguration httpConfig = super.createHttpConfig(requiresHostnameVerification);
        httpConfig.addCustomizer(new SecureRequestCustomizer(requiresHostnameVerification));
        httpConfig.addCustomizer(requestCustomizer);
        return httpConfig;
    }

    public ServerConnector createConnector(
            Server server, SslPolicy sslPolicy, SocketAddress address, JettyThreadCalculator jettyThreadCalculator) {
        var httpConfig = createHttpConfig(sslPolicy.isVerifyHostname());
        var connectionFactories = new ArrayList<ConnectionFactory>();
        var http11 = new JettyHttpConnectionFactory(connectionTracker, httpConfig);

        if (configuration.get(ServerSettings.http_enabled_transports).contains(ConfigurableTransports.HTTP2)) {
            var httpsConnectionListener = new JettyHttp2ConnectionListener(connectionTracker, HTTPS2_NAME);

            var http2 = new HTTP2ServerConnectionFactory(httpConfig);
            http2.addBean(httpsConnectionListener);
            var alpn = new ALPNServerConnectionFactory();

            SslConnectionFactory sslConnectionFactory = createSslConnectionFactory(sslPolicy, alpn.getProtocol());

            connectionFactories.add(sslConnectionFactory);
            connectionFactories.add(alpn);
            connectionFactories.add(http2);

            if (configuration.get(ServerSettings.http_enabled_transports).contains(ConfigurableTransports.HTTP1_1)) {
                alpn.setDefaultProtocol(http11.getProtocol());
                connectionFactories.add(http11);
            }
        } else if (configuration
                .get(ServerSettings.http_enabled_transports)
                .equals(Set.of(ConfigurableTransports.HTTP1_1))) {
            SslConnectionFactory sslConnectionFactory = createSslConnectionFactory(sslPolicy, http11.getProtocol());

            connectionFactories.add(sslConnectionFactory);
            connectionFactories.add(http11);
        }

        return createConnector(server, address, jettyThreadCalculator, connectionFactories);
    }

    private static SslConnectionFactory createSslConnectionFactory(SslPolicy sslPolicy, String nextProtocol) {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();

        String password = UUID.randomUUID().toString();
        sslContextFactory.setKeyStore(sslPolicy.getKeyStore(password.toCharArray(), password.toCharArray()));
        sslContextFactory.setKeyStorePassword(password);
        sslContextFactory.setKeyManagerPassword(password);

        List<String> ciphers = sslPolicy.getCipherSuites();
        if (ciphers != null) {
            sslContextFactory.setIncludeCipherSuites(ciphers.toArray(new String[0]));
            sslContextFactory.setExcludeCipherSuites();
        }

        String[] protocols = sslPolicy.getTlsVersions();
        if (protocols != null) {
            sslContextFactory.setIncludeProtocols(protocols);
            sslContextFactory.setExcludeProtocols();
        }

        switch (sslPolicy.getClientAuth()) {
            case REQUIRE:
                sslContextFactory.setNeedClientAuth(true);
                break;
            case OPTIONAL:
                sslContextFactory.setWantClientAuth(true);
                break;
            case NONE:
                sslContextFactory.setWantClientAuth(false);
                sslContextFactory.setNeedClientAuth(false);
                break;
            default:
                throw new IllegalArgumentException("Not supported: " + sslPolicy.getClientAuth());
        }

        return new SslConnectionFactory(sslContextFactory, nextProtocol);
    }
}
