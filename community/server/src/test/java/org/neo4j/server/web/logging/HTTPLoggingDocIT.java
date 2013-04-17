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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.osIsWindows;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.MapBasedConfiguration;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.preflight.EnsurePreparedForHttpLogging;
import org.neo4j.server.preflight.HTTPLoggingPreparednessRuleTest;
import org.neo4j.server.preflight.PreflightFailedException;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class HTTPLoggingDocIT extends ExclusiveServerTestBase
{
    private NeoServer server;
    private static File logDirectory = null;

    @Before
    public void setUp() throws IOException
    {
        ServerHelper.cleanTheDatabase( server );
        removeHttpLogs();
    }

    private void removeHttpLogs() throws IOException
    {
        if ( logDirectory != null && logDirectory.exists() )
        {
            FileUtils.deleteDirectory( logDirectory );
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
            .usingDatabaseDir( folder.getRoot().getAbsolutePath() )
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
        logDirectory = TargetDirectory.forTest( this.getClass() ).directory( "logdir" );
        FileUtils.forceMkdir( logDirectory );
        final File confDir = TargetDirectory.forTest( this.getClass() ).directory( "confdir" );
        FileUtils.forceMkdir( confDir );

        final File configFile = HTTPLoggingPreparednessRuleTest.createConfigFile(
            HTTPLoggingPreparednessRuleTest.createLogbackConfigXml( logDirectory ), confDir );

        server = ServerBuilder.server().withDefaultDatabaseTuning()
            .withProperty( Configurator.HTTP_LOGGING, "true" )
            .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
            .usingDatabaseDir( folder.getRoot().getAbsolutePath() )
            .build();
        server.start();

        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

        // when
        String query = "?explicitlyEnabled=" + UUID.randomUUID().toString();
        JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
        assertEquals( 200, response.getStatus() );
        response.close();

        // then
        final File outputLog = new File( logDirectory + File.separator + "http.log" );
        assertTrue( occursIn( query, outputLog ) );
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
        config.setProperty(Configurator.HTTP_LOGGING, "true");
        config.setProperty(Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath());
        
        server = ServerBuilder.server().withDefaultDatabaseTuning()
            .withPreflightTasks( new EnsurePreparedForHttpLogging(config) )
            .withProperty( Configurator.HTTP_LOGGING, "true" )
            .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION, configFile.getPath() )
            .usingDatabaseDir( folder.getRoot().getAbsolutePath() )
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


//    private String createLogbackConfigFile( File logDirectory )
//    {
//        return "<configuration>\n" +
//            "  <appender name=\"FILE\" class=\"ch.qos.logback.core.rolling.RollingFileAppender\">\n" +
//            "    <file>" + logDirectory.getAbsolutePath() + File.separator + "http.log</file>\n" +
//            "    <rollingPolicy class=\"ch.qos.logback.core.rolling.TimeBasedRollingPolicy\">\n" +
//            "      <fileNamePattern>" + logDirectory.getAbsolutePath() + File.separator + "http.%d{yyyy-MM-dd_HH}.log</fileNamePattern>\n" +
//            "      <maxHistory>30</maxHistory>\n" +
//            "    </rollingPolicy>\n" +
//            "\n" +
//            "    <encoder>\n" +
//            "      <!-- Note the deliberate misspelling of \"referer\" in accordance with RFC1616 -->\n" +
//            "      <pattern>%h %l %user [%t{dd/MMM/yyyy:HH:mm:ss Z}] \"%r\" %s %b \"%i{Referer}\" \"%i{User-Agent}\"</pattern>\n" +
//            "    </encoder>\n" +
//            "  </appender>\n" +
//            "\n" +
//            "  <appender-ref ref=\"FILE\" />\n" +
//            "</configuration>";
//    }
}
