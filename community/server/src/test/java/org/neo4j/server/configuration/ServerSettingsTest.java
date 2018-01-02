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
package org.neo4j.server.configuration;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.parboiled.common.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.configuration.ServerSettings.http_log_config_file;
import static org.neo4j.server.configuration.ServerSettings.http_logging_enabled;
import static org.neo4j.server.configuration.ServerSettings.webserver_https_cert_path;
import static org.neo4j.server.configuration.ServerSettings.webserver_https_key_path;

public class ServerSettingsTest
{
    @Rule
    public TemporaryFolder dir = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldConvertWebserverTLSToDBMSTLS() throws Throwable
    {
        // Given
        final AssertableLogProvider logging = new AssertableLogProvider();

        // When
        final Config config = new Config( stringMap(
                webserver_https_cert_path.name(), "cert",
                webserver_https_key_path.name(), "key" ), ServerSettings.class );
        config.setLogger( logging.getLog( "config" ) );

        // Then
        assertEquals( "cert", config.get( ServerSettings.tls_certificate_file ).getPath() );
        assertEquals( "key", config.get( ServerSettings.tls_key_file ).getPath() );
        logging.assertContainsMessageContaining(
                "The TLS certificate configuration you are using, 'org.neo4j.server.webserver.https.cert.location' " +
                "is deprecated. Please use 'dbms.security.tls_certificate_file' instead." );
        logging.assertContainsMessageContaining(
                "The TLS key configuration you are using, 'org.neo4j.server.webserver.https.key.location' " +
                "is deprecated, please use 'dbms.security.tls_key_file' instead." );
    }

    @Test
    public void shouldAllowWritableLogFileTarget() throws Throwable
    {
        // Given
        File configFile = createHttpLogConfig( dir.newFile( "logfile" ) );

        Config config = new Config( stringMap(
                http_logging_enabled.name(), TRUE,
                http_log_config_file.name(), configFile.getAbsolutePath() ), ServerSettings.class );

        // When
        File file = config.get( http_log_config_file );

        // Then
        assertThat( file.getAbsoluteFile(), equalTo( configFile.getAbsoluteFile() ) );
    }

    @Test
    public void shouldFailToValidateHttpLogFileWithInvalidLogFileName() throws Throwable
    {
        // Given
        File logFile = new File( createUnwritableDirectory(), "logfile" );
        File configFile = createHttpLogConfig( logFile );


        // Expect
        exception.expect( InvalidSettingException.class );

        // When
        Config config = new Config( stringMap(
                http_logging_enabled.name(), TRUE,
                http_log_config_file.name(), configFile.getAbsolutePath() ), ServerSettings.class );

    }

    private File createHttpLogConfig( File logFile ) throws IOException
    {
        File configFile = dir.newFile( "http-logging.xml" );
        FileUtils.writeAllText(
                "<configuration>\n" +
                "  <appender name=\"FILE\" class=\"ch.qos.logback.core.rolling.RollingFileAppender\">\n" +
                "    <file>" + logFile.getAbsolutePath() + "</file>\n" +
                "    <rollingPolicy class=\"ch.qos.logback.core.rolling.TimeBasedRollingPolicy\">\n" +
                "      <fileNamePattern>/var/log/neo4j/http.%d{yyyy-MM-dd_HH}.log</fileNamePattern>\n" +
                "      <maxHistory>7</maxHistory>\n" +
                "    </rollingPolicy>\n" +
                "\n" +
                "    <encoder>\n" +
                "      <!-- Note the deliberate misspelling of \"referer\" in accordance with RFC1616 -->\n" +
                "      <pattern>%h %l %user [%t{dd/MMM/yyyy:HH:mm:ss Z}] \"%r\" %s %b \"%i{Referer}\" \"%i{User-Agent}\" %D</pattern>\n" +
                "    </encoder>\n" +
                "  </appender>\n" +
                "\n" +
                "  <appender-ref ref=\"FILE\"/>\n" +
                "</configuration>\n" +
                "\n", configFile, StandardCharsets.UTF_8 );
        return configFile;
    }

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
}
