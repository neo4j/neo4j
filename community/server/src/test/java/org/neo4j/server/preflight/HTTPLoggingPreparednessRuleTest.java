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
package org.neo4j.server.preflight;

import org.apache.commons.lang.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.TargetDirectory;

import static org.neo4j.server.configuration.ServerSettings.http_log_config_file;
import static org.neo4j.server.configuration.ServerSettings.http_logging_enabled;

public class HTTPLoggingPreparednessRuleTest
{
    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldPassWhenExplicitlyDisabled()
    {
        // given
        Config config = new Config( MapUtil.stringMap( http_logging_enabled.name(), "false" ) );

        // when
        config.get( http_log_config_file );

        // then no config error occurs
    }

    @Test
    public void shouldPassWhenImplicitlyDisabled()
    {
        // given
        Config config = new Config();

        // when
        config.get( http_log_config_file );

        // then no config error occurs
    }

    @Test
    public void shouldPassWhenEnabledWithGoodConfigSpecified() throws Exception
    {
        // given
        File logDir = testDirectory.directory( "logDir" );
        File confDir = testDirectory.directory( "confDir" );
        Config config = new Config( MapUtil.stringMap( http_logging_enabled.name(), "true",
                http_log_config_file.name(), createConfigFile(
                        createLogbackConfigXml( logDir ), confDir ).getAbsolutePath() ) );

        // when
        config.get( http_log_config_file );

        // then no config error occurs
    }

    @Test
    public void shouldFailWhenEnabledWithUnwritableLogDirSpecifiedInConfig() throws Exception
    {
        // given
        File confDir = testDirectory.directory( "confDir" );
        File unwritableDirectory = createUnwritableDirectory();
        Config config = new Config( MapUtil.stringMap(
                http_logging_enabled.name(), "true",
                http_log_config_file.name(), createConfigFile( createLogbackConfigXml( unwritableDirectory ), confDir ).getAbsolutePath() ) );

        // expect
        exception.expect( InvalidSettingException.class );

        // when
        config.get( http_log_config_file );

    }

    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    public static File createUnwritableDirectory()
    {
        File file;
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            file = new File( "\\\\" + UUID.randomUUID().toString() + "\\" );
        }
        else if ( SystemUtils.IS_OS_MAC_OSX )
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
