/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.preflight;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.Settings.osIsMacOS;
import static org.neo4j.helpers.Settings.osIsWindows;

public class HTTPLoggingPreparednessRuleTest
{
    @Test
    public void shouldPassWhenExplicitlyDisabled()
    {
        // given
        Config config = new Config( MapUtil.stringMap( Configurator.HTTP_LOGGING, "false" ) );
        EnsurePreparedForHttpLogging rule = new EnsurePreparedForHttpLogging( config );

        // when
        boolean result = rule.run();

        // then
        assertTrue( result );
        assertEquals( StringUtils.EMPTY, rule.getFailureMessage() );
    }

    @Test
    public void shouldPassWhenImplicitlyDisabled()
    {
        // given
        Config config = new Config();
        EnsurePreparedForHttpLogging rule = new EnsurePreparedForHttpLogging( config );

        // when
        boolean result = rule.run();

        // then
        assertTrue( result );
        assertEquals( StringUtils.EMPTY, rule.getFailureMessage() );
    }

    @Test
    public void shouldPassWhenEnabledWithGoodConfigSpecified() throws Exception
    {
        // given
        File logDir = TargetDirectory.forTest( this.getClass() ).cleanDirectory( "logDir" );
        File confDir = TargetDirectory.forTest( this.getClass() ).cleanDirectory( "confDir" );
        Config config = new Config( MapUtil.stringMap( Configurator.HTTP_LOGGING, "true",
                Configurator.HTTP_LOG_CONFIG_LOCATION, createConfigFile( createLogbackConfigXml( logDir ), confDir )
                        .getAbsolutePath() ) );
        EnsurePreparedForHttpLogging rule = new EnsurePreparedForHttpLogging( config );

        // when
        boolean result = rule.run();
        // then
        assertTrue( result );
        assertEquals( StringUtils.EMPTY, rule.getFailureMessage() );
    }

    @Test
    public void shouldFailWhenEnabledWithUnwritableLogDirSpecifiedInConfig() throws Exception
    {
        // given
        File confDir = TargetDirectory.forTest( this.getClass() ).cleanDirectory( "confDir" );
        File unwritableDirectory = createUnwritableDirectory();
        Config config = new Config( MapUtil.stringMap( Configurator.HTTP_LOGGING, "true",
                Configurator.HTTP_LOG_CONFIG_LOCATION,
                createConfigFile( createLogbackConfigXml( unwritableDirectory ), confDir ).getAbsolutePath() ) );
        EnsurePreparedForHttpLogging rule = new EnsurePreparedForHttpLogging( config );

        // when
        boolean result = rule.run();

        // then
        assertFalse( result );
        assertEquals( String.format( "HTTP log directory [%s] does not exist", unwritableDirectory ),
                rule.getFailureMessage() );
    }

    public static File createUnwritableDirectory()
    {
        File file;
        if ( osIsWindows() )
        {
            file = new File( "\\\\" + UUID.randomUUID().toString() + "\\" );
        }
        else if ( osIsMacOS() )
        {
            file = new File( "/Network/Servers/localhost/" + UUID.randomUUID().toString() );
        }
        else
        {
            file = new File( "/proc/" + UUID.randomUUID().toString() + "/random" );
        }
        return file;
    }

    public static File createConfigFile( String configXml, File location ) throws IOException
    {
        File configFile = new File( location.getAbsolutePath() + File.separator + "neo4j-logback-config.xml" );
        FileOutputStream fos = new FileOutputStream( configFile );
        fos.write( configXml.getBytes() );
        fos.close();
        return configFile;
    }

    public static String createLogbackConfigXml( File logDirectory )
    {
        return createLogbackConfigXml( logDirectory, "%h %l %user [%t{dd/MMM/yyyy:HH:mm:ss Z}] \"%r\" %s %b \"%i{Referer}\" \"%i{User-Agent}\"" );
    }

    public static String createLogbackConfigXml( File logDirectory, String logPattern )
    {
        return "<configuration>\n" +
                "  <appender name=\"FILE\" class=\"ch.qos.logback.core.rolling.RollingFileAppender\">\n" +
                "    <file>" + logDirectory.getAbsolutePath() + File.separator + "http.log</file>\n" +
                "    <rollingPolicy class=\"ch.qos.logback.core.rolling.TimeBasedRollingPolicy\">\n" +
                "      <fileNamePattern>" + logDirectory.getAbsolutePath() + File.separator + "http.%d{yyyy-MM-dd_HH}.log</fileNamePattern>\n" +
                "      <maxHistory>30</maxHistory>\n" +
                "    </rollingPolicy>\n" +
                "\n" +
                "    <encoder>\n" +
                "      <!-- Note the deliberate misspelling of \"referer\" in accordance with RFC 2616 -->\n" +
                "      <pattern>"+logPattern+"</pattern>\n" +
                "    </encoder>\n" +
                "  </appender>\n" +
                "\n" +
                "  <appender-ref ref=\"FILE\" />\n" +
                "</configuration>";
    }
}
