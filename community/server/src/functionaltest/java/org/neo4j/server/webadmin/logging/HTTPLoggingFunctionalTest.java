/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.webadmin.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.osIsWindows;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.startup.healthcheck.HTTPLoggingPreparednessRule;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckFailedException;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class HTTPLoggingFunctionalTest extends ExclusiveServerTestBase
{
    private NeoServer server;
    private static final String logDirectory = "target/test-data/impermanent-db/log";

    @Before
    public void cleanUp() throws IOException
    {
        ServerHelper.cleanTheDatabase( server );
        removeHttpLogs();
    }

    private void removeHttpLogs() throws IOException
    {
        File logDir = new File( logDirectory );
        if ( logDir.exists() )
        {
            FileUtils.deleteDirectory( logDir );
        }
    }

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses() throws Exception
    {
        // given
        server = ServerBuilder.server().withDefaultDatabaseTuning()
            .withProperty( Configurator.HTTP_LOGGING, "false" )
            .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION,
                getClass().getResource( "/neo4j-server-test-logback.xml" ).getFile() )
            .build();
        server.start();
        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

        // when
        String query = "?implicitlyDisabled" + UUID.randomUUID().toString();
        JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
        assertEquals( 200, response.getStatus() );
        response.close();

        // then
        assertFalse( occursIn( query, new File( logDirectory + File.separator + "http.log" ) ) );
    }

    @Test
    public void givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess() throws Exception
    {
        // given
        server = ServerBuilder.server().withDefaultDatabaseTuning()
            .withProperty( Configurator.HTTP_LOGGING, "true" )
            .withProperty( Configurator.HTTP_LOG_LOCATION, logDirectory )
            .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION,
                getClass().getResource( "/neo4j-server-test-logback.xml" ).getFile() )
            .build();
        server.start();

        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

        // when
        String query = "?explicitlyEnabled=" + UUID.randomUUID().toString();
        JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
        assertEquals( 200, response.getStatus() );
        response.close();

        // then
        assertTrue( occursIn( query, new File( logDirectory + File.separator + "http.log" ) ) );
    }

    @Test
    public void givenConfigurationWithUnwritableLogDirectoryShouldFailToStartServer() throws Exception
    {
        // Apparently you cannot create an unwritable directory in Windows with
        // neither File#setWritable nor creating a directory in the root.
        assumeTrue( !osIsWindows() );

        // given
        final String unwritableLogDir = createUnwritableDirectory().getAbsolutePath();
        server = ServerBuilder.server().withDefaultDatabaseTuning()
            .withStartupHealthCheckRules( new HTTPLoggingPreparednessRule() )
            .withProperty( Configurator.HTTP_LOGGING, "true" )
            .withProperty( Configurator.HTTP_LOG_LOCATION, unwritableLogDir )
            .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION,
                getClass().getResource( "/neo4j-server-test-logback.xml" ).getFile() )
            .build();

        // when
        try
        {
            server.start();
            fail( "should have thrown exception" );
        }
        catch ( StartupHealthCheckFailedException e )
        {
            // then
            assertThat( e.getMessage(),
                containsString( String.format( "HTTP log directory [%s] is not writable", unwritableLogDir ) ) );
        }

    }

    @Test
    public void givenConfigurationWithInvalidLogDirectoryShouldFailToStartServer() throws Exception
    {
        // given
        final String invalidLogDir = createInvalidDirectory().getAbsolutePath();
        server = ServerBuilder.server().withDefaultDatabaseTuning()
            .withStartupHealthCheckRules( new HTTPLoggingPreparednessRule() )
            .withProperty( Configurator.HTTP_LOGGING, "true" )
            .withProperty( Configurator.HTTP_LOG_LOCATION, invalidLogDir )
            .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION,
                getClass().getResource( "/neo4j-server-test-logback.xml" ).getFile() )
            .build();

        // when
        try
        {
            server.start();
            fail( "should have thrown exception" );
        }
        catch ( StartupHealthCheckFailedException e )
        {
            // then
            assertThat( e.getMessage(),
                containsString( String.format( "HTTP log directory [%s] cannot be created", invalidLogDir ) ) );
        }
    }

    private File createInvalidDirectory() throws IOException
    {
        File directory = TargetDirectory.forTest( this.getClass() ).directory( "invalid" );
        File file = new File( directory, "file" );
        file.createNewFile();
        return new File( file, "subdirectory" );
    }

    private File createUnwritableDirectory()
    {
        File file;
        if ( osIsWindows() )
        {
            file = new File( "\\\\" + UUID.randomUUID().toString() + "\\" );
        }
        else
        {
            TargetDirectory targetDirectory = TargetDirectory.forTest( this.getClass() );

            file = targetDirectory.file( "unwritable-" + System.currentTimeMillis() );
            file.mkdirs();
            file.setWritable( false, false );
        }

        return file;
    }

    private boolean occursIn( String lookFor, File file ) throws FileNotFoundException
    {
        if ( !file.exists() )
        {
            return false;
        }

        boolean result = false;
        Scanner scanner = new Scanner( file );
        while ( scanner.hasNext() )
        {
            if ( scanner.next().contains( lookFor ) )
            {
                result = true;
            }
        }

        scanner.close();

        return result;
    }
}
