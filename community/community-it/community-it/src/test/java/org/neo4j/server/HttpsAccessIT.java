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
package org.neo4j.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.GET;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import java.security.SecureRandom;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.PortUtils;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.InsecureTrustManager;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

class HttpsAccessIT extends ExclusiveWebContainerTestBase {
    private SSLSocketFactory originalSslSocketFactory;
    private TestWebContainer testWebContainer;

    @BeforeEach
    void setUp() {
        originalSslSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
    }

    @AfterEach
    void tearDown() {
        HttpsURLConnection.setDefaultSSLSocketFactory(originalSslSocketFactory);
        testWebContainer.shutdown();
    }

    @Test
    void serverShouldSupportSsl() throws Exception {
        startServer();

        assertThat(GET(testWebContainer.httpsUri().get().toString()).status()).isEqualTo(200);
        assertThat(GET(testWebContainer.getBaseUri().toString()).status()).isEqualTo(200);
    }

    @Test
    void txEndpointShouldReplyWithHttpsWhenItReturnsURLs() throws Exception {
        startServer();

        String baseUri = testWebContainer.getBaseUri().toString();
        HTTP.Response response = POST(baseUri + txEndpoint(), quotedJson("{'statements':[]}"));

        assertThat(response.location()).startsWith(baseUri);
        assertThat(response.get("commit").asText()).startsWith(baseUri);
    }

    @Test
    void shouldExposeBaseUriWhenHttpEnabledAndHttpsDisabled() throws Exception {
        startServer(true, false);
        shouldInstallConnector("http", ConnectorType.HTTP);
        shouldExposeCorrectSchemeInDiscoveryService("http");
    }

    @Test
    void shouldExposeBaseUriWhenHttpDisabledAndHttpsEnabled() throws Exception {
        startServer(false, true);
        shouldInstallConnector("https", ConnectorType.HTTPS);
        shouldExposeCorrectSchemeInDiscoveryService("https");
    }

    @Test
    void shouldIncludeStrictTransportHeaderIfConfigured() throws Exception {
        var transportHeader = "max-age=31536000";
        startServer(false, true, transportHeader, "localhost", false);
        shouldInstallConnector("https", ConnectorType.HTTPS);

        var response = GET(testWebContainer.getBaseUri().toString());

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.header(HttpHeader.STRICT_TRANSPORT_SECURITY.lowerCaseName()))
                .isEqualTo(transportHeader);
    }

    @Test
    void shouldNotVerifyCertificateHostname() throws Exception {
        startSecureServer("thishostnameiswrong", false);
        assertThat(GET(testWebContainer.httpsUri().get().toString()).status()).isEqualTo(200);
    }

    @Test
    void shouldVerifyIncorrectCertificateHostname() throws Exception {
        startSecureServer("thishostnameiswrong", true);
        var response = GET(testWebContainer.httpsUri().get().toString());
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void shouldVerifyCorrectCertificateHostname() throws Exception {
        startSecureServer("localhost", true);
        assertThat(GET(testWebContainer.httpsUri().get().toString()).status()).isEqualTo(200);
    }

    private void shouldInstallConnector(String scheme, ConnectorType connectorType) {
        var uri = testWebContainer.getBaseUri();
        assertEquals(scheme, uri.getScheme());
        HostnamePort expectedHostPort =
                PortUtils.getConnectorAddress(testWebContainer.getDefaultDatabase(), connectorType);
        assertEquals(expectedHostPort.getHost(), uri.getHost());
        assertEquals(expectedHostPort.getPort(), uri.getPort());
    }

    private void shouldExposeCorrectSchemeInDiscoveryService(String scheme) throws Exception {
        var response = GET(testWebContainer.getBaseUri().toString());

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.stringFromContent("transaction")).startsWith(scheme + "://");
    }

    private void startServer() throws Exception {
        startServer(true, true);
    }

    private void startServer(boolean httpEnabled, boolean httpsEnabled) throws Exception {
        startServer(httpEnabled, httpsEnabled, null, "localhost", false);
    }

    private void startSecureServer(String certHostname, boolean hostNameCheck) throws Exception {
        startServer(false, true, null, certHostname, hostNameCheck);
    }

    private void startServer(
            boolean httpEnabled,
            boolean httpsEnabled,
            String strictTransportHeader,
            String certHostName,
            boolean hostNameCheck)
            throws Exception {
        CommunityWebContainerBuilder serverBuilder = serverOnRandomPorts()
                .persistent()
                .usingDataDir(
                        testDirectory.directory(methodName).toAbsolutePath().toString());
        if (!httpEnabled) {
            serverBuilder.withHttpDisabled();
        }
        if (httpsEnabled) {
            var certificates = testDirectory.absolutePath().resolve("certificates");
            SelfSignedCertificateFactory.create(new DefaultFileSystemAbstraction(), certificates, certHostName);
            SslPolicyConfig policy = SslPolicyConfig.forScope(SslPolicyScope.HTTPS);
            serverBuilder.withProperty(policy.enabled.name(), Boolean.TRUE.toString());
            serverBuilder.withProperty(
                    policy.base_directory.name(), certificates.toAbsolutePath().toString());
            serverBuilder.withProperty(policy.trust_all.name(), SettingValueParsers.TRUE);
            serverBuilder.withProperty(policy.client_auth.name(), ClientAuth.NONE.name());
            serverBuilder.withProperty(policy.verify_hostname.name(), hostNameCheck ? "true" : "false");
            serverBuilder.withHttpsEnabled();
        }
        if (strictTransportHeader != null) {
            serverBuilder.withProperty(ServerSettings.http_strict_transport_security.name(), strictTransportHeader);
        }

        testWebContainer = serverBuilder.build();

        // Because we are generating a non-CA-signed certificate, we need to turn off verification in the client.
        // This is ironic, since there is no proper verification on the CA side in the first place, but I digress.
        TrustManager[] trustAllCerts = {new InsecureTrustManager()};

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(null, trustAllCerts, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    }
}
