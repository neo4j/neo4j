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
package org.neo4j.server.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.logging.log4j.LogConfig.HTTP_LOG;
import static org.neo4j.logging.log4j.LogConfig.STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE;
import static org.neo4j.logging.log4j.LogUtils.newLoggerBuilder;
import static org.neo4j.logging.log4j.LogUtils.newXmlConfigBuilder;
import static org.neo4j.logging.log4j.LoggerTarget.HTTP_LOGGER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.AbstractNeoWebServer;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

class HttpStructuredLoggingIT extends ExclusiveWebContainerTestBase {
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    @Test
    void shouldLogRequestsInStructuredFormat() throws Exception {
        testDirectory.directory("logs");
        Path log4jConfig = testDirectory.file("logs/testHttp.xml");
        Path httpLogPath = testDirectory.file("logs/" + HTTP_LOG);
        newXmlConfigBuilder(testDirectory.getFileSystem(), log4jConfig)
                .withLogger(newLoggerBuilder(HTTP_LOGGER, httpLogPath)
                        .withJsonFormatTemplate(STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE)
                        .build())
                .create();

        var bootstrapper = new CommunityBootstrapper();
        HttpResponse<String> response;
        try {
            int start = bootstrapper.start(
                    testDirectory.homePath(),
                    Map.of(
                            HttpConnector.listen_address.name(),
                            "localhost:0",
                            HttpConnector.advertised_address.name(),
                            "localhost:0",
                            ServerSettings.http_logging_enabled.name(),
                            TRUE,
                            HttpConnector.enabled.name(),
                            TRUE,
                            GraphDatabaseSettings.server_logging_config_path.name(),
                            log4jConfig.toString()));
            assertThat(start).isEqualTo(0);

            var dependencyResolver = getDependencyResolver(bootstrapper.getDatabaseManagementService());
            var baseUri = dependencyResolver
                    .resolveDependency(AbstractNeoWebServer.class)
                    .getBaseUri();
            var config = dependencyResolver.resolveDependency(Config.class);

            var request = HttpRequest.newBuilder()
                    .uri(baseUri)
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .header("User-Agent", HttpStructuredLoggingIT.class.getSimpleName())
                    .GET()
                    .build();

            // Just ask the discovery api for a response we don't actually care of
            response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        } finally {
            bootstrapper.stop();

            // Make sure the log manager flushes everything.
            LogManager.shutdown();
        }
        assertThat(response.statusCode()).isEqualTo(200);

        List<String> strings = Files.readAllLines(httpLogPath);
        var httpLogLines = strings.stream()
                .map(s -> {
                    try {
                        return OBJECT_MAPPER.readValue(s, MAP_TYPE);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        assertThat(httpLogLines).anyMatch(logEntry -> logEntry.getOrDefault("message", "")
                .contains(HttpStructuredLoggingIT.class.getSimpleName()));
    }

    private static DependencyResolver getDependencyResolver(DatabaseManagementService managementService) {
        return ((GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME)).getDependencyResolver();
    }
}
