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
package org.neo4j.server.web.logging;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static java.net.http.HttpClient.Redirect.NORMAL;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.server.helpers.CommunityServerBuilder.serverOnRandomPorts;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class HTTPLoggingIT extends ExclusiveServerTestBase
{

    private final ExpectedException exception = ExpectedException.none();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final TestName testName = new TestName();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( exception )
                                          .around( testName ).around( testDirectory );

    @Test
    public void givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses() throws Exception
    {
        // given
        var directoryPrefix = testName.getMethodName();
        var logDirectory = testDirectory.directory( directoryPrefix + "-logdir" );

        var server = serverOnRandomPorts().withDefaultDatabaseTuning().persistent()
                .withProperty( ServerSettings.http_logging_enabled.name(), FALSE )
                .withProperty( GraphDatabaseSettings.logs_directory.name(), logDirectory.toString() )
                .withProperty( BoltConnector.listen_address.name(), ":0" )
                .usingDataDir( testDirectory.directory( directoryPrefix + "-dbdir" ).getAbsolutePath() )
                .build();
        try
        {
            server.start();
            var functionalTestHelper = new FunctionalTestHelper( server );

            // when
            var query = "?implicitlyDisabled" + randomString();
            var response = queryBaseUri( query, functionalTestHelper );
            assertThat( response.statusCode(), is( OK_200 ) );

            // then
            var httpLogFile = new File( logDirectory, "http.log" );
            assertThat( httpLogFile.exists(), is( false ) );
        }
        finally
        {
            server.stop();
        }
    }

    @Test
    public void givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess() throws Exception
    {
        // given
        var directoryPrefix = testName.getMethodName();
        var logDirectory = testDirectory.directory( directoryPrefix + "-logdir" );
        var query = "?explicitlyEnabled=" + randomString();

        var server = serverOnRandomPorts().withDefaultDatabaseTuning().persistent()
                .withProperty( ServerSettings.http_logging_enabled.name(), TRUE )
                .withProperty( GraphDatabaseSettings.logs_directory.name(), logDirectory.getAbsolutePath() )
                .withProperty( BoltConnector.listen_address.name(), ":0" )
                .usingDataDir( testDirectory.directory( directoryPrefix + "-dbdir" ).getAbsolutePath() )
                .build();
        try
        {
            server.start();

            var functionalTestHelper = new FunctionalTestHelper( server );

            // when
            var response = queryBaseUri( query, functionalTestHelper );
            assertThat( response.statusCode(), is( OK_200 ) );

            // then
            var httpLogFile = new File( logDirectory, "http.log" );
            assertEventually( "request appears in log", fileContentSupplier( httpLogFile ), containsString( query ), 5, TimeUnit.SECONDS );
        }
        finally
        {
            server.stop();
        }
    }

    private static ThrowingSupplier<String,IOException> fileContentSupplier( File file )
    {
        return () -> Files.readString( file.toPath() );
    }

    private static String randomString()
    {
        return UUID.randomUUID().toString();
    }

    private static HttpResponse<Void> queryBaseUri( String query, FunctionalTestHelper functionalTestHelper ) throws IOException, InterruptedException
    {
        var request = HttpRequest.newBuilder( URI.create( functionalTestHelper.baseUri() + query ) ).GET().build();
        var httpClient = HttpClient.newBuilder().followRedirects( NORMAL ).build();
        return httpClient.send( request, discarding() );
    }
}
