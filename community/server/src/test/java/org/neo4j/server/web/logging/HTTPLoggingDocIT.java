/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.preflight.EnsurePreparedForHttpLogging;
import org.neo4j.server.preflight.HTTPLoggingPreparednessRuleTest;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static org.neo4j.io.fs.FileUtils.readTextFile;
import static org.neo4j.test.Assert.assertEventually;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

public class HTTPLoggingDocIT extends ExclusiveServerTestBase
{
    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses() throws Exception
    {
        // given
        File logDirectory = testDirectory.directory(
                "givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses-logdir" );
        FileUtils.forceMkdir( logDirectory );
        final File confDir = testDirectory.directory(
                "givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses-confdir" );
        FileUtils.forceMkdir( confDir );

        final File configFile = HTTPLoggingPreparednessRuleTest.createConfigFile(
                HTTPLoggingPreparednessRuleTest.createLogbackConfigXml( logDirectory ), confDir );

        NeoServer server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withProperty( Configurator.HTTP_LOGGING, "false" )
                .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
                .usingDatabaseDir( testDirectory.directory(
                        "givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses-dbdir"
                ).getAbsolutePath() )
                .build();
        try
        {
            server.start();
            FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

            // when
            String query = "?implicitlyDisabled" + randomString();
            JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
            assertThat( response.getStatus(), is( 200 ) );
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
        final File logDirectory =
                testDirectory.directory( "givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess-logdir" );
        FileUtils.forceMkdir( logDirectory );
        final File confDir =
                testDirectory.directory( "givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess-confdir" );
        FileUtils.forceMkdir( confDir );

        final File configFile = HTTPLoggingPreparednessRuleTest.createConfigFile(
                HTTPLoggingPreparednessRuleTest.createLogbackConfigXml( logDirectory ), confDir );

        final String query = "?explicitlyEnabled=" + randomString();

        NeoServer server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withProperty( Configurator.HTTP_LOGGING, "true" )
                .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
                .usingDatabaseDir( testDirectory.directory(
                        "givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess-dbdir"
                ).getAbsolutePath() )
                .build();
        try
        {
            server.start();

            FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

            // when
            JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
            assertThat( response.getStatus(), is( 200 ) );
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

    @Test
    public void givenDebugContentLoggingEnabledShouldLogContent() throws Exception
    {
        // given
        final File logDirectory = testDirectory.directory( "givenDebugContentLoggingEnabledShouldLogContent-logdir" );
        FileUtils.forceMkdir( logDirectory );
        final File confDir = testDirectory.directory( "givenDebugContentLoggingEnabledShouldLogContent-confdir" );
        FileUtils.forceMkdir( confDir );

        final File configFile = HTTPLoggingPreparednessRuleTest.createConfigFile(
                HTTPLoggingPreparednessRuleTest.createLogbackConfigXml( logDirectory, "$requestContent\n%responseContent" ), confDir );

        NeoServer server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withProperty( Configurator.HTTP_LOGGING, "true" )
                .withProperty( Configurator.HTTP_CONTENT_LOGGING, "true" )
                .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
                .usingDatabaseDir( testDirectory.directory( "givenDebugContentLoggingEnabledShouldLogContent-dbdir" )
                        .getAbsolutePath() )
                .build();

        try
        {
            server.start();

            // when
            HTTP.Response req = HTTP.POST( server.baseUri().resolve( "/db/data/node" ).toString(), rawPayload( "{\"name\":\"Hello, world!\"}" ) );
            assertThat( req.status(), is( 201 ) );

            // then
            File httpLog = new File( logDirectory, "http.log" );
            assertEventually( "request appears in log", fileContentSupplier( httpLog ), containsString( "Hello, world!" ), 5, TimeUnit.SECONDS );
            assertEventually( "request appears in log", fileContentSupplier( httpLog ), containsString( "metadata" ), 5, TimeUnit.SECONDS );
        }
        finally
        {
            server.stop();
        }
    }

    @Test
    public void givenConfigurationWithUnwritableLogDirectoryShouldFailToStartServer() throws Exception
    {
        // given
        final File confDir = testDirectory.directory( "confdir" );
        final File unwritableLogDir = createUnwritableDirectory();

        final File configFile = HTTPLoggingPreparednessRuleTest.createConfigFile(
                HTTPLoggingPreparednessRuleTest.createLogbackConfigXml( unwritableLogDir ), confDir );

        Config config = new Config( MapUtil.stringMap(
                Configurator.HTTP_LOGGING, "true",
                Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() ) );

        // expect
        exception.expect( InvalidSettingException.class );

        // when
        NeoServer server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withPreflightTasks( new EnsurePreparedForHttpLogging( config ) )
                .withProperty( Configurator.HTTP_LOGGING, "true" )
                .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
                .usingDatabaseDir( confDir.getAbsolutePath() )
                .build();
    }

    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private ThrowingSupplier<String, IOException> fileContentSupplier( final File file )
    {
        return new ThrowingSupplier<String, IOException>()
        {
            @Override
            public String get() throws IOException
            {
                return readTextFile( file, Charsets.UTF_8 );
            }
        };
    }

    private File createUnwritableDirectory()
    {
        File file;
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            file = new File( "\\\\" + randomString() + "\\http.log" );
        }
        else
        {
            file = testDirectory.file( "unwritable-" + randomString() );
            assertThat( "create directory to be unwritable", file.mkdirs(), is( true ) );

            // Assume that we can change the file permissions, and that permissions are respected.
            // If these checks fail, then we cannot use the current file system for this test, so we bail.
            assumeThat( "mark directory as unwritable", file.setWritable( false, false ), is( true ) );
            assumeThat( "directory permissions are respected", file.canWrite(), is( false ) );
        }

        return file;
    }

    private String randomString()
    {
        return UUID.randomUUID().toString();
    }
}
