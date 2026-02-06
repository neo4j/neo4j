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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class ZippedBrowserIT {

    @Test
    public void testBrowserZip() throws IOException, InterruptedException {
        var dbms = setupDatabase("neo4j-browser.zip");

        var client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        var request = HttpRequest.newBuilder()
                .uri(URI.create(databaseUrl(dbms) + "browser/"))
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("<h1>New Improved Browser</h1>\n" + "<h2>Designed by Oskar</h2>\n");
        dbms.shutdown();
    }

    @Test
    public void testBrowserZipRedirect() throws IOException, InterruptedException {
        var dbms = setupDatabase("neo4j-browser.zip");
        var client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        var request = HttpRequest.newBuilder()
                .uri(URI.create(databaseUrl(dbms)))
                .header("Accept", "text/html")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("<h1>New Improved Browser</h1>\n" + "<h2>Designed by Oskar</h2>\n");
        dbms.shutdown();
    }

    @Test
    public void testBrowserZipWithDifferentFileName() throws IOException, InterruptedException {
        var dbms = setupDatabase("neo4j-browser-2025.01.24+0.zip");
        var client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        var request = HttpRequest.newBuilder()
                .uri(URI.create(databaseUrl(dbms)))
                .header("Accept", "text/html")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body())
                .isEqualTo("<h1>Browser With Different Zip Name</h1>\n" + "<h2>Designed by Oskar</h2>\n");
        dbms.shutdown();
    }

    @Test
    public void shouldServeLatestWhenMultipleAvailable() throws IOException, InterruptedException {
        var dbms = setupDatabase("neo4j-browser-2026.01.01+0.zip", "neo4j-browser-2027.01.01+0.zip");
        var client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        var request = HttpRequest.newBuilder()
                .uri(URI.create(databaseUrl(dbms)))
                .header("Accept", "text/html")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("<h1>V2 With New Amazing Features</h1>\n");
        dbms.shutdown();
    }

    private String databaseUrl(DatabaseManagementService database) {
        return "http://"
                + ((GraphDatabaseAPI) database.database("neo4j"))
                        .getDependencyResolver()
                        .resolveDependency(ConnectorPortRegister.class)
                        .getLocalAddress(ConnectorType.HTTP)
                + "/";
    }

    private static void configureBrowserDir(Path homeDirectory, String... zipFiles) throws IOException {
        var webDirectory = homeDirectory.resolve("web");
        Files.createDirectories(webDirectory);

        for (String zipFile : zipFiles) {
            var zippedDirectory = webDirectory.resolve(zipFile);
            Files.copy(Objects.requireNonNull(ZippedBrowserIT.class.getResourceAsStream(zipFile)), zippedDirectory);
        }
    }

    private DatabaseManagementService setupDatabase(String... zipFiles) throws IOException {
        var builder = new TestDatabaseManagementServiceBuilder();
        var tempDir = Files.createTempDirectory("tempDir");
        configureBrowserDir(tempDir, zipFiles);

        return builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(BoltConnector.enabled, true)
                .setDatabaseRootDirectory(tempDir)
                .build();
    }
}
