/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.UUID;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.MapBasedConfiguration;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.preflight.EnsurePreparedForHttpLogging;
import org.neo4j.server.preflight.HTTPLoggingPreparednessRuleTest;
import org.neo4j.server.preflight.PreflightFailedException;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.Settings.osIsWindows;

public class HTTPLoggingDocIT extends ExclusiveServerTestBase
{
    @Test
    public void givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses() throws Exception
    {
        // given
        File logDirectory = TargetDirectory.forTest( this.getClass() ).directory(
                "givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses-logdir", true );
        FileUtils.forceMkdir( logDirectory );
        final File confDir = TargetDirectory.forTest( this.getClass() ).directory(
                "givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses-confdir", true );
        FileUtils.forceMkdir( confDir );

        final File configFile = HTTPLoggingPreparednessRuleTest.createConfigFile(
                HTTPLoggingPreparednessRuleTest.createLogbackConfigXml( logDirectory ), confDir );

        NeoServer server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withProperty( Configurator.HTTP_LOGGING, "false" )
                .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
                .usingDatabaseDir( TargetDirectory.forTest( this.getClass() ).directory(
                        "givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses-dbdir",
                        true ).getAbsolutePath() )
                .build();
        try
        {
            server.start();
            FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

            // when
            String query = "?implicitlyDisabled" + UUID.randomUUID().toString();
            JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
            assertEquals( 200, response.getStatus() );
            response.close();

            // then
            assertFalse( occursIn( query, new File( logDirectory, "http.log" ) ) );
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
        File logDirectory = TargetDirectory.forTest( this.getClass() ).directory(
                "givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess-logdir", true );
        FileUtils.forceMkdir( logDirectory );
        final File confDir = TargetDirectory.forTest( this.getClass() ).directory(
                "givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess-confdir", true );
        FileUtils.forceMkdir( confDir );

        final File configFile = HTTPLoggingPreparednessRuleTest.createConfigFile(
                HTTPLoggingPreparednessRuleTest.createLogbackConfigXml( logDirectory ), confDir );

        String query = "?explicitlyEnabled=" + UUID.randomUUID().toString();

        NeoServer server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withProperty( Configurator.HTTP_LOGGING, "true" )
                .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
                .usingDatabaseDir( TargetDirectory.forTest( this.getClass() ).directory(
                        "givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess-dbdir",
                        true ).getAbsolutePath() )
                .build();
        try
        {
            server.start();

            FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

            // when
            JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
            assertEquals( 200, response.getStatus() );
            response.close();

            // then
            final File outputLog = new File( logDirectory, "http.log" );
            assertTrue( occursIn( query, outputLog ) );

        }
        catch ( Throwable t )
        {
            t.printStackTrace();
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
        final File confDir = TargetDirectory.forTest( this.getClass() ).directory( "confdir" );
        FileUtils.forceMkdir( confDir );
        final File unwritableLogDir = createUnwritableDirectory();

        final File configFile = HTTPLoggingPreparednessRuleTest.createConfigFile(
                HTTPLoggingPreparednessRuleTest.createLogbackConfigXml( unwritableLogDir ), confDir );

        Configuration config = new MapBasedConfiguration();
        config.setProperty( Configurator.HTTP_LOGGING, "true" );
        config.setProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() );

        NeoServer server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withPreflightTasks( new EnsurePreparedForHttpLogging( config ) )
                .withProperty( Configurator.HTTP_LOGGING, "true" )
                .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
                .usingDatabaseDir( TargetDirectory.forTest( this.getClass() ).directory( "confdir" ).getAbsolutePath() )
                .build();

        // when
        try
        {
            server.start();
            fail( "should have thrown exception" );
        }
        catch ( PreflightFailedException e )
        {
            // then
            assertThat( e.getMessage(),
                    containsString( String.format( "HTTP log directory [%s]",
                            unwritableLogDir.getAbsolutePath() ) ) );
        }
        finally
        {
            server.stop();
        }
    }

    private File createUnwritableDirectory()
    {
        File file;
        if ( osIsWindows() )
        {
            file = new File( "\\\\" + UUID.randomUUID().toString() + "\\http.log" );
        }
        else
        {
            TargetDirectory targetDirectory = TargetDirectory.forTest( this.getClass() );

            file = targetDirectory.file( "unwritable-" + System.currentTimeMillis() );
            assertTrue( "create directory to be unwritable", file.mkdirs() );
            assertTrue( "mark directory as unwritable", file.setWritable( false, false ) );
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
