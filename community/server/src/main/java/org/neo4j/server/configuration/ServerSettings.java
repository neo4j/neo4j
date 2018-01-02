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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.configuration.Migrator;

import static org.neo4j.kernel.configuration.Settings.ANY;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.EMPTY;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.illegalValueMessage;
import static org.neo4j.kernel.configuration.Settings.matches;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.port;
import static org.neo4j.kernel.configuration.Settings.setting;

@Description("Settings used by the server configuration")
public interface ServerSettings
{
    @Migrator
    ConfigurationMigrator migrator = new ServerConfigurationMigrator();

    @Description( "Maximum request header size" )
    Setting<Integer> maximum_request_header_size =
            setting( "org.neo4j.server.webserver.max.request.header", INTEGER, "20480" );

    @Description( "Maximum response header size" )
    Setting<Integer> maximum_response_header_size =
            setting( "org.neo4j.server.webserver.max.response.header", INTEGER, "20480" );

    @Description( "Comma-seperated list of custom security rules for Neo4j to use." )
    Setting<List<String>> security_rules = setting( "org.neo4j.server.rest.security_rules",
            STRING_LIST, EMPTY );

    // webserver configuration
    @Description( "Http port for the Neo4j REST API." )
    Setting<Integer> webserver_port = setting( "org.neo4j.server.webserver.port", INTEGER, "7474",
            port );

    @Description( "Hostname for the Neo4j REST API" )
    Setting<String> webserver_address = setting( "org.neo4j.server.webserver.address", STRING,
            "localhost", illegalValueMessage( "Must be a valid hostname", matches( ANY ) ) );

    @Description( "Number of Neo4j worker threads." )
    Setting<Integer> webserver_max_threads = setting( "org.neo4j.server.webserver.maxthreads",
            INTEGER, ""+Math.min(Runtime.getRuntime().availableProcessors(),500), min( 1 ) );

    @Description( "If execution time limiting is enabled in the database, this configures the maximum request execution time." )
    Setting<Long> webserver_limit_execution_time = setting(
            "org.neo4j.server.webserver.limit.executiontime", DURATION, NO_DEFAULT );

    @Deprecated
    @Description( "Path to the statistics database file. RRDB has been deprecate, please use the Metrics plugin instead." )
    Setting<File> rrdb_location = setting( "org.neo4j.server.webadmin.rrdb.location", PATH, NO_DEFAULT );

    @Description( "Console engines for the legacy webadmin administration" )
    Setting<List<String>> management_console_engines = setting(
            "org.neo4j.server.manage.console_engines", STRING_LIST, "SHELL" );

    @Description( "Comma-separated list of <classname>=<mount point> for unmanaged extensions." )
    Setting<List<ThirdPartyJaxRsPackage>> third_party_packages = setting( "org.neo4j.server.thirdparty_jaxrs_classes",
            new Function<String, List<ThirdPartyJaxRsPackage>>()
            {
                @Override
                public List<ThirdPartyJaxRsPackage> apply( String value )
                {
                    String[] list = value.split( Settings.SEPARATOR );
                    List<ThirdPartyJaxRsPackage> result = new ArrayList<>();
                    for ( String item : list )
                    {
                        item = item.trim();
                        if ( !item.equals( "" ) )
                        {
                            result.add( createThirdPartyJaxRsPackage( item ) );
                        }
                    }
                    return result;
                }

                @Override
                public String toString()
                {
                    return "a comma-seperated list of <classname>=<mount point> strings";
                }

                private ThirdPartyJaxRsPackage createThirdPartyJaxRsPackage( String packageAndMoutpoint )
                {
                    String[] parts = packageAndMoutpoint.split( "=" );
                    if ( parts.length != 2 )
                    {
                        throw new IllegalArgumentException( "config for " + ServerSettings.third_party_packages.name()
                                + " is wrong: " + packageAndMoutpoint );
                    }
                    String pkg = parts[0];
                    String mountPoint = parts[1];
                    return new ThirdPartyJaxRsPackage( pkg, mountPoint );
                }
            },
            EMPTY );

    @Description( "Enable HTTPS for the REST API." )
    Setting<Boolean> webserver_https_enabled = setting( "org.neo4j.server.webserver.https.enabled", BOOLEAN, FALSE );

    @Description( "HTTPS port for the REST API." )
    Setting<Integer> webserver_https_port = setting( "org.neo4j.server.webserver.https.port", INTEGER, "7473", port );

    @Description( "Path to the X.509 public certificate to be used by Neo4j for TLS connections" )
    Setting<File> tls_certificate_file = setting(
            "dbms.security.tls_certificate_file", PATH, "neo4j-home/ssl/snakeoil.cert" );

    @Description( "Path to the X.509 private key to be used by Neo4j for TLS connections" )
    Setting<File> tls_key_file = setting(
            "dbms.security.tls_key_file", PATH, "neo4j-home/ssl/snakeoil.key" );

    @Deprecated
    @Description( "Path to the SSL certificate used for HTTPS connections. This is deprecated, please use " +
                  "'dbms.security.tls_certificate_file' instead." )
    Setting<File> webserver_https_cert_path = setting(
            "org.neo4j.server.webserver.https.cert.location", PATH, "neo4j-home/ssl/snakeoil.cert" );

    @Deprecated
    @Description( "Path to the SSL key used for HTTPS connections. This is deprecated, please use " +
                  "'dbms.security.tls_key_file'" )
    Setting<File> webserver_https_key_path = setting(
            "org.neo4j.server.webserver.https.key.location", PATH, "neo4j-home/ssl/snakeoil.key" );


    @Description( "Enable HTTP request logging." )
    Setting<Boolean> http_logging_enabled = setting( "org.neo4j.server.http.log.enabled", BOOLEAN, FALSE );

    @Description( "Enable HTTP content logging." )
    Setting<Boolean> http_content_logging_enabled = setting( "org.neo4j.server.http.unsafe.content_log.enabled",
            BOOLEAN, FALSE );

    @Description( "Path to a logback configuration file for HTTP request logging." )
    Setting<File> http_log_config_file = setting( "org.neo4j.server.http.log.config", new HttpLogSetting(), NO_DEFAULT );

    @Description( "Timeout for idle transactions." )
    Setting<Long> transaction_timeout = setting( "org.neo4j.server.transaction.timeout", DURATION, "60s" );

    @Description( "Enable auth requirement to access Neo4j." )
    Setting<Boolean> auth_enabled = setting("dbms.security.auth_enabled", BOOLEAN, TRUE);

    @Internal
    @Description("Enable Bolt")
    Setting<Boolean> bolt_enabled = setting( "xx.bolt.enabled", BOOLEAN, FALSE );

    @Internal
    @Description("Enable TLS for Bolt")
    Setting<Boolean> bolt_tls_enabled = setting( "xx.bolt.tls.enabled", BOOLEAN, FALSE );

    @Internal
    @Description("Host and port for Bolt")
    Setting<HostnamePort> bolt_socket_address = setting( "dbms.bolt.address", HOSTNAME_PORT, "localhost:7687" );

    @Internal
    @Description("Host and port for the Bolt Websocket")
    Setting<HostnamePort> bolt_ws_address = setting( "dbms.bolt.ws.address", HOSTNAME_PORT, "localhost:7688" );

}
