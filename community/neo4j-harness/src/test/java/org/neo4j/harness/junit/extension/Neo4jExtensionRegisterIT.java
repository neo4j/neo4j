/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.harness.junit.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.harness.junit.Neo4j;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.server.HTTP;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.server.ServerTestUtils.getSharedTestTemporaryFolder;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

class Neo4jExtensionRegisterIT
{
    private static final String REGISTERED_TEMP_PREFIX = "registeredTemp";
    @RegisterExtension
    static Neo4jExtension neo4jExtension = Neo4jExtension.builder()
                    .withFolder( createTempDirectory() )
                    .withFixture( "CREATE (u:User)" )
                    .withConfig( GraphDatabaseSettings.db_timezone.name(), LogTimeZone.SYSTEM.toString() )
                    .withConfig( LegacySslPolicyConfig.certificates_directory.name(),
                    getRelativePath( getSharedTestTemporaryFolder(), LegacySslPolicyConfig.certificates_directory ) )
                    .withFixture( graphDatabaseService ->
                        {
                            try ( Transaction tx = graphDatabaseService.beginTx() )
                            {
                                graphDatabaseService.createNode( Label.label( "User" ) );
                                tx.success();
                            }
                            return null;
                        } )
                    .withUnmanagedExtension( "/test", MyUnmanagedExtension.class )
                    .build();

    @Test
    void neo4jAvailable( Neo4j neo4j )
    {
        assertNotNull( neo4j );
        assertThat( HTTP.GET( neo4j.httpURI().toString() ).status(), equalTo( 200 ) );
    }

    @Test
    void graphDatabaseServiceIsAvailable( GraphDatabaseService databaseService )
    {
        assertNotNull( databaseService );
        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = databaseService.beginTx() )
            {
                databaseService.createNode();
                transaction.success();
            }
        } );
    }

    @Test
    void shouldUseSystemTimeZoneForLogging( GraphDatabaseService databaseService ) throws Exception
    {
        String currentOffset = currentTimeZoneOffsetString();

        assertThat( contentOf( "neo4j.log", databaseService ), containsString( currentOffset ) );
        assertThat( contentOf( "debug.log", databaseService), containsString( currentOffset ) );
    }

    @Test
    void customExtensionWorkingDirectory( Neo4j neo4j )
    {
        assertThat( neo4j.config().get( GraphDatabaseSettings.data_directory ).getParentFile().getName(), startsWith( REGISTERED_TEMP_PREFIX ) );
    }

    @Test
    void unmanagedExtensionRegistered( Neo4j neo4j )
    {
        assertThat( HTTP.GET( neo4j.httpURI().resolve( "test/myExtension" ).toString() ).status(), equalTo( 234 ) );
    }

    @Test
    void fixturesRegistered( Neo4j neo4j ) throws Exception
    {
        // Then
        HTTP.Response response = HTTP.POST( neo4j.httpURI().toString() + "db/data/transaction/commit",
                quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

        assertThat( response.get( "results" ).get( 0 ).get( "data" ).size(), equalTo( 2 ) );
    }

    private static String currentTimeZoneOffsetString()
    {
        ZoneOffset offset = OffsetDateTime.now().getOffset();
        return offset.equals( UTC ) ? "+0000" : offset.toString().replace( ":", "" );
    }

    private String contentOf( String file, GraphDatabaseService databaseService ) throws IOException
    {
        GraphDatabaseAPI api = (GraphDatabaseAPI) databaseService;
        Config config = api.getDependencyResolver().resolveDependency( Config.class );
        File dataDirectory = config.get( GraphDatabaseSettings.data_directory );
        return new String( Files.readAllBytes( new File( dataDirectory, file ).toPath() ), UTF_8 );
    }

    private static File createTempDirectory()
    {
        try
        {
            return Files.createTempDirectory( REGISTERED_TEMP_PREFIX ).toFile();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
