/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.commons.compress.utils.Charsets;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.transport.integration.AuthenticationIT;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.helpers.collection.MapUtil.map;

public class EnterpriseAuthenticationIT extends AuthenticationIT
{
    private File securityLog;

    @Override
    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    @Override
    protected Consumer<Map<String, String>> getSettingsFunction()
    {
        final Path homeDir;
        try
        {
            homeDir = Files.createTempDirectory( "logs" );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Test setup failed to create temporary directory", e );
        }
        securityLog = new File( homeDir.toFile(), "security.log" );

        return settings -> {
            settings.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
            settings.put( GraphDatabaseSettings.logs_directory.name(), homeDir.toAbsolutePath().toString() );
        };
    }

    @Override
    public void shouldFailIfMalformedAuthTokenUnknownScheme() throws Throwable
    {
        // Ignore this test in enterprise since custom schemes may be allowed
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldLogInitPasswordChange() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "neo4j", "new_credentials", "secret", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );

        server.graphDatabaseService().shutdown(); // to force writing of async logs

        FullLog log = new FullLog();
        log.assertHasLine( "neo4j", "changed password" );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldLogFailedInitPasswordChange() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "neo4j", "new_credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.General.InvalidArguments,
                "Old password and new password cannot be the same.") ) );

        server.graphDatabaseService().shutdown(); // to force writing of async logs

        FullLog log = new FullLog();
        log.assertHasLine( "neo4j", "tried to change password: Old password and new password cannot be the same." );
    }

    private class FullLog
    {
        List<String> lines;

        FullLog() throws IOException
        {
            lines = new ArrayList<>();
            BufferedReader bufferedReader = new BufferedReader(
                    fsRule.get().openAsReader(
                            new File( securityLog.getAbsolutePath() ),
                            Charsets.UTF_8
                    ) );
            lines.add( bufferedReader.readLine() );
            bufferedReader.lines().forEach( lines::add );
        }

        void assertHasLine( String subject, String msg )
        {
            assertThat( lines, hasItem( containsString( "[" + subject + "]: " + msg ) ) );
        }
    }
}
