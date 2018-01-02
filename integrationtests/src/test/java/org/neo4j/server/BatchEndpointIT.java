/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.server.configuration.ServerSettings;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.Response;
import static org.neo4j.test.server.HTTP.withBaseUri;

public class BatchEndpointIT
{
    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withConfig( ServerSettings.http_logging_enabled, "true" )
            .withConfig( ServerSettings.http_log_config_file, createDummyLogbackConfigFile() )
            .withConfig( ServerSettings.auth_enabled, "false" );

    @Test
    public void requestsShouldNotFailWhenHttpLoggingIsOn()
    {
        // Given
        String body = "[" +
                      "{'method': 'POST', 'to': '/node', 'body': {'age': 1}, 'id': 1}," +
                      "{'method': 'POST', 'to': '/node', 'body': {'age': 2}, 'id': 2}" +
                      "]";

        // When
        Response response = withBaseUri( neo4j.httpURI().toString() )
                .withHeaders( "Content-Type", "application/json" )
                .POST( "db/data/batch", quotedJson( body ) );

        // Then
        assertEquals( 200, response.status() );
    }

    private static String createDummyLogbackConfigFile()
    {
        try
        {
            Path file = Files.createTempFile( "logback", ".xml" );
            Files.write( file, "<configuration></configuration>".getBytes() );
            return file.toAbsolutePath().toString();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to create dummy logback configuration file", e );
        }
    }
}
