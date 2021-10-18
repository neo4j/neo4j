/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.server.AbstractNeoWebServer;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

class HttpStructuredLoggingIT extends ExclusiveWebContainerTestBase
{
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
                                                            .version( HttpClient.Version.HTTP_1_1 )
                                                            .followRedirects( HttpClient.Redirect.NORMAL )
                                                            .build();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String,String>> MAP_TYPE = new TypeReference<>()
    {
    };

    @Test
    void shouldLogRequestsInStructuredFormat( ) throws Exception
    {
        var bootstrapper = new CommunityBootstrapper();
        HttpResponse<String> response;
        Path httpLogPath;
        try
        {
            bootstrapper.start( testDirectory.homePath(), Map.of(
                    HttpConnector.listen_address.name(), "localhost:0",
                    ServerSettings.http_log_format.name(), FormattedLogFormat.JSON.name(),
                    ServerSettings.http_logging_enabled.name(), TRUE,
                    HttpConnector.enabled.name(), TRUE
            ) );
            var dependencyResolver = getDependencyResolver( bootstrapper.getDatabaseManagementService() );
            var baseUri = dependencyResolver.resolveDependency( AbstractNeoWebServer.class ).getBaseUri();
            var config = dependencyResolver.resolveDependency( Config.class );

            var request = HttpRequest.newBuilder()
                                     .uri( baseUri )
                                     .timeout( Duration.ofSeconds( 10 ) )
                                     .header( "Accept", "application/json" )
                                     .header( "User-Agent", HttpStructuredLoggingIT.class.getSimpleName() )
                                     .GET()
                                     .build();

            // Just ask the discovery api for a response we don't actually care of
            httpLogPath = config.get( ServerSettings.http_log_path );
            response = HTTP_CLIENT.send( request, HttpResponse.BodyHandlers.ofString() );
        }
        finally
        {
            bootstrapper.stop();

            // Make sure the log manager flushes everything.
            LogManager.shutdown();
        }
        assertThat( response.statusCode() ).isEqualTo( 200 );

        var httpLogLines =
                Files.readAllLines( httpLogPath ).stream()
                     .map( s ->
                           {
                               try
                               {
                                   return OBJECT_MAPPER.readValue( s, MAP_TYPE );
                               }
                               catch ( JsonProcessingException e )
                               {
                                   throw new RuntimeException( e );
                               }
                           } )
                     .collect( Collectors.toList() );

        assertThat( httpLogLines )
                .anyMatch( logEntry -> logEntry.getOrDefault( "message", "" ).contains( HttpStructuredLoggingIT.class.getSimpleName() ) );
    }

    private static DependencyResolver getDependencyResolver( DatabaseManagementService managementService )
    {
        return ((GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME )).getDependencyResolver();
    }
}
