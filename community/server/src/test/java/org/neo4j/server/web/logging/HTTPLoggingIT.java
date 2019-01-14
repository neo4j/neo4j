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

import org.apache.http.HttpStatus;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.neo4j.io.fs.FileUtils.readTextFile;
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
        String directoryPrefix = testName.getMethodName();
        File logDirectory = testDirectory.directory( directoryPrefix + "-logdir" );

        NeoServer server = serverOnRandomPorts().withDefaultDatabaseTuning().persistent()
                .withProperty( ServerSettings.http_logging_enabled.name(), Settings.FALSE )
                .withProperty( GraphDatabaseSettings.logs_directory.name(), logDirectory.toString() )
                .withProperty( new BoltConnector( "bolt" ).listen_address.name(), ":0" )
                .usingDataDir( testDirectory.directory( directoryPrefix + "-dbdir" ).getAbsolutePath() )
                .build();
        try
        {
            server.start();
            FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

            // when
            String query = "?implicitlyDisabled" + randomString();
            JaxRsResponse response = new RestRequest().get( functionalTestHelper.managementUri() + query );

            assertThat( response.getStatus(), is( HttpStatus.SC_OK ) );
            response.close();

            // then
            File httpLog = new File( logDirectory, "http.log" );
            assertThat( httpLog.exists(), is( false ) );
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
        String directoryPrefix = testName.getMethodName();
        File logDirectory = testDirectory.directory( directoryPrefix + "-logdir" );
        final String query = "?explicitlyEnabled=" + randomString();

        NeoServer server = serverOnRandomPorts().withDefaultDatabaseTuning().persistent()
                .withProperty( ServerSettings.http_logging_enabled.name(), Settings.TRUE )
                .withProperty( GraphDatabaseSettings.logs_directory.name(), logDirectory.getAbsolutePath() )
                .withProperty( new BoltConnector( "bolt" ).listen_address.name(), ":0" )
                .usingDataDir( testDirectory.directory( directoryPrefix + "-dbdir" ).getAbsolutePath() )
                .build();
        try
        {
            server.start();

            FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

            // when
            JaxRsResponse response = new RestRequest().get( functionalTestHelper.managementUri() + query );
            assertThat( response.getStatus(), is( HttpStatus.SC_OK ) );
            response.close();

            // then
            File httpLog = new File( logDirectory, "http.log" );
            assertEventually( "request appears in log", fileContentSupplier( httpLog ), containsString( query ), 5, TimeUnit.SECONDS );
        }
        finally
        {
            server.stop();
        }
    }

    private ThrowingSupplier<String, IOException> fileContentSupplier( final File file )
    {
        return () -> readTextFile( file, StandardCharsets.UTF_8 );
    }

    private String randomString()
    {
        return UUID.randomUUID().toString();
    }
}
