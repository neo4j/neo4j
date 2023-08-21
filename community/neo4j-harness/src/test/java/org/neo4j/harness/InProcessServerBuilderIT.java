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
package org.neo4j.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.ssl.SslPolicyScope.HTTPS;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.server.WebContainerTestUtils.verifyConnector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class InProcessServerBuilderIT {
    @Inject
    private TestDirectory directory;

    @Test
    void shouldLaunchAServerInSpecifiedDirectory() throws IOException {
        // Given
        Path workDir = directory.directory("specific");

        // When
        try (Neo4j neo4j = getTestBuilder(workDir).build()) {
            // Then
            assertThat(HTTP.GET(neo4j.httpURI().toString()).status()).isEqualTo(200);
            try (Stream<Path> list = Files.list(workDir)) {
                assertThat(list.count()).isEqualTo(1);
            }
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertTrue(FileUtils.isDirectoryEmpty(workDir));
    }

    @Test
    void shouldAllowCustomServerAndDbConfig() throws Exception {
        // Given
        trustAllSSLCerts();

        // Get default trusted cypher suites
        SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        List<String> defaultCiphers = Arrays.asList(ssf.getDefaultCipherSuites());

        // When
        SslPolicyConfig pem = SslPolicyConfig.forScope(HTTPS);

        var certificates = directory.directory("certificates");
        SelfSignedCertificateFactory.create(directory.getFileSystem(), certificates, "private.key", "public.crt");
        Files.createDirectories(certificates.resolve("trusted"));
        Files.createDirectories(certificates.resolve("revoked"));

        try (Neo4j neo4j = getTestBuilder(directory.homePath())
                .withConfig(HttpConnector.enabled, true)
                .withConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .withConfig(HttpsConnector.enabled, true)
                .withConfig(HttpsConnector.listen_address, new SocketAddress("localhost", 0))
                .withConfig(GraphDatabaseSettings.dense_node_threshold, 20)
                // override legacy policy
                .withConfig(pem.enabled, Boolean.TRUE)
                .withConfig(pem.base_directory, certificates)
                .withConfig(pem.ciphers, defaultCiphers)
                .withConfig(pem.tls_versions, List.of("TLSv1.2", "TLSv1.1", "TLSv1"))
                .withConfig(pem.client_auth, ClientAuth.NONE)
                .withConfig(pem.trust_all, true)
                .build()) {
            // Then
            assertThat(HTTP.GET(neo4j.httpURI().toString()).status()).isEqualTo(200);
            assertThat(HTTP.GET(neo4j.httpsURI().toString()).status()).isEqualTo(200);
            Config config = ((GraphDatabaseAPI) neo4j.defaultDatabaseService())
                    .getDependencyResolver()
                    .resolveDependency(Config.class);
            assertEquals(20, config.get(GraphDatabaseSettings.dense_node_threshold));
        }
    }

    @Test
    void shouldMountUnmanagedExtensionsByClass() {
        // When
        try (Neo4j neo4j = getTestBuilder(directory.homePath())
                .withUnmanagedExtension("/path/to/my/extension", MyUnmanagedExtension.class)
                .build()) {
            // Then
            assertThat(HTTP.GET(neo4j.httpURI() + "path/to/my/extension/myExtension")
                            .status())
                    .isEqualTo(234);
        }
    }

    @Test
    void shouldMountUnmanagedExtensionsByPackage() {
        // When
        try (Neo4j neo4j = getTestBuilder(directory.homePath())
                .withUnmanagedExtension("/path/to/my/extension", "org.neo4j.harness.extensionpackage")
                .build()) {
            // Then
            assertThat(HTTP.GET(neo4j.httpURI() + "path/to/my/extension/myExtension")
                            .status())
                    .isEqualTo(234);
        }
    }

    @Test
    void startWithDisabledServer() {
        try (Neo4j neo4j =
                getTestBuilder(directory.homePath()).withDisabledServer().build()) {
            assertThrows(IllegalStateException.class, neo4j::httpURI);
            assertThrows(IllegalStateException.class, neo4j::httpsURI);

            assertDoesNotThrow(() -> {
                GraphDatabaseService service = neo4j.defaultDatabaseService();
                try (Transaction transaction = service.beginTx()) {
                    transaction.createNode();
                    transaction.commit();
                }
            });
        }
    }

    @Test
    void shouldFindFreePort() {
        // Given one server is running
        try (Neo4j firstServer = getTestBuilder(directory.homePath()).build()) {
            // When I build a second server
            try (Neo4j secondServer = getTestBuilder(directory.homePath()).build()) {
                // Then
                assertThat(secondServer.httpURI().getPort())
                        .isNotEqualTo(firstServer.httpURI().getPort());
            }
        }
    }

    @Test
    void shouldRunBuilderOnExistingStoreDir() {
        // When
        // create graph db with one node upfront
        Path existingHomeDir = directory.homePath("existingStore");

        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(existingHomeDir).build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
        try (Transaction transaction = db.beginTx()) {
            transaction.execute("create ()");
            transaction.commit();
        } finally {
            managementService.shutdown();
        }

        try (Neo4j neo4j =
                getTestBuilder(existingHomeDir).copyFrom(existingHomeDir).build()) {
            // Then
            GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
            try (Transaction tx = graphDatabaseService.beginTx()) {
                assertTrue(Iterables.count(tx.getAllNodes()) > 0);

                // When: create another node
                tx.createNode();
                tx.commit();
            }
        }

        // Then: we still only have one node since the server is supposed to work on a copy
        managementService = new TestDatabaseManagementServiceBuilder(existingHomeDir).build();
        db = managementService.database(DEFAULT_DATABASE_NAME);
        try {
            try (Transaction tx = db.beginTx()) {
                assertEquals(1, Iterables.count(tx.getAllNodes()));
                tx.commit();
            }
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void shouldOpenBoltPort() {
        // given
        try (Neo4j neo4j = getTestBuilder(directory.homePath()).build()) {
            URI uri = neo4j.boltURI();

            // when
            assertDoesNotThrow(() -> {
                try (var connection = new SocketConnection(new InetSocketAddress(uri.getHost(), uri.getPort()))) {
                    connection.connect();
                }
            });
        }
    }

    @Test
    void shouldFailWhenProvidingANonDirectoryAsSource() throws IOException {
        Path notADirectory = Files.createTempFile("prefix", "suffix");
        assertFalse(Files.isDirectory(notADirectory));

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> getTestBuilder(directory.homePath())
                        .copyFrom(notADirectory)
                        .build());
        assertThat(exception.getMessage()).contains("is not a directory");
    }

    @Test
    void shouldStartServerWithHttpHttpsAndBoltDisabled() {
        testStartupWithConnectors(false, false, false);
    }

    @Test
    void shouldStartServerWithHttpEnabledAndHttpsBoltDisabled() {
        testStartupWithConnectors(true, false, false);
    }

    @Test
    void shouldStartServerWithHttpsEnabledAndHttpBoltDisabled() {
        testStartupWithConnectors(false, true, false);
    }

    @Test
    void shouldStartServerWithBoltEnabledAndHttpHttpsDisabled() {
        testStartupWithConnectors(false, false, true);
    }

    @Test
    void shouldStartServerWithHttpHttpsEnabledAndBoltDisabled() {
        testStartupWithConnectors(true, true, false);
    }

    @Test
    void shouldStartServerWithHttpBoltEnabledAndHttpsDisabled() {
        testStartupWithConnectors(true, false, true);
    }

    @Test
    void shouldStartServerWithHttpsBoltEnabledAndHttpDisabled() {
        testStartupWithConnectors(false, true, true);
    }

    private void testStartupWithConnectors(boolean httpEnabled, boolean httpsEnabled, boolean boltEnabled) {
        var certificates = directory.directory("certificates");
        Neo4jBuilder serverBuilder = Neo4jBuilders.newInProcessBuilder(directory.homePath())
                .withConfig(HttpConnector.enabled, httpEnabled)
                .withConfig(HttpConnector.listen_address, new SocketAddress(0))
                .withConfig(HttpsConnector.enabled, httpsEnabled)
                .withConfig(HttpsConnector.listen_address, new SocketAddress(0))
                .withConfig(BoltConnector.enabled, boltEnabled)
                .withConfig(BoltConnector.listen_address, new SocketAddress(0));

        if (httpsEnabled) {
            SelfSignedCertificateFactory.create(directory.getFileSystem(), certificates);
            serverBuilder.withConfig(SslPolicyConfig.forScope(HTTPS).enabled, Boolean.TRUE);
            serverBuilder.withConfig(SslPolicyConfig.forScope(HTTPS).base_directory, certificates);
        }

        try (Neo4j neo4j = serverBuilder.build()) {
            GraphDatabaseService db = neo4j.defaultDatabaseService();

            assertDbAccessible(db);
            verifyConnector(db, ConnectorType.HTTP, httpEnabled);
            verifyConnector(db, ConnectorType.HTTPS, httpsEnabled);
            verifyConnector(db, ConnectorType.BOLT, boltEnabled);
        }
    }

    private static void assertDbAccessible(GraphDatabaseService db) {
        Label label = () -> "Person";
        String propertyKey = "name";
        String propertyValue = "Thor Odinson";

        try (Transaction tx = db.beginTx()) {
            tx.createNode(label).setProperty(propertyKey, propertyValue);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            Node node = single(tx.findNodes(label));
            assertEquals(propertyValue, node.getProperty(propertyKey));
            tx.commit();
        }
    }

    private static void trustAllSSLCerts() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[] {
            new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] arg0, String arg1) {}

                @Override
                public void checkServerTrusted(X509Certificate[] arg0, String arg1) {}

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            }
        };

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(null, trustAllCerts, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    }

    private static Neo4jBuilder getTestBuilder(Path workDir) {
        return Neo4jBuilders.newInProcessBuilder(workDir);
    }
}
