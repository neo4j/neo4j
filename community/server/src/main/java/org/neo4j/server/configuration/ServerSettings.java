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
package org.neo4j.server.configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Settings;

import static org.neo4j.helpers.Settings.ANY;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.FALSE;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.EMPTY;
import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.STRING_LIST;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.illegalValueMessage;
import static org.neo4j.helpers.Settings.matches;
import static org.neo4j.helpers.Settings.min;
import static org.neo4j.helpers.Settings.port;
import static org.neo4j.helpers.Settings.setting;

@Description("Settings used by the server configuration")
public interface ServerSettings
{

    @Description( "Comma-seperated list of custom security rules for Neo4j to use." )
    public static final Setting<List<String>> security_rules = setting( "org.neo4j.server.rest.security_rules",
            STRING_LIST, EMPTY );

    // webserver configuration
    @Description( "Http port for the Neo4j REST API." )
    public static final Setting<Integer> webserver_port = setting( "org.neo4j.server.webserver.port", INTEGER, "7474",
            port );

    @Description( "Hostname for the Neo4j REST API" )
    public static final Setting<String> webserver_address = setting( "org.neo4j.server.webserver.address", STRING,
            "localhost", illegalValueMessage( "Must be a valid hostname", matches( ANY ) ) );

    @Description( "Number of Neo4j worker threads." )
    public static final Setting<Integer> webserver_max_threads = setting( "org.neo4j.server.webserver.maxthreads",
            INTEGER, NO_DEFAULT, min( 1 ) );

    @Description( "If execution time limiting is enabled in the database, this configures the maximum request execution time." )
    public static final Setting<Long> webserver_limit_execution_time = setting(
            "org.neo4j.server.webserver.limit.executiontime", DURATION, NO_DEFAULT );

    // other settings
    @Description( "Path to the statistics database file." )
    public static final Setting<File> rrdb_location = setting( "org.neo4j.server.webadmin.rrdb.location", PATH,
            NO_DEFAULT );

    @Description( "Console engines for the legacy webadmin administr" )
    public static final Setting<List<String>> management_console_engines = setting(
            "org.neo4j.server.manage.console_engines", STRING_LIST, "SHELL" );

    @Description( "Comma-separated list of <classname>=<mount point> for unmanaged extensions." )
    public static final Setting<List<ThirdPartyJaxRsPackage>> third_party_packages = setting( "org.neo4j.server.thirdparty_jaxrs_classes",
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

    // security configuration
    @Description( "Enable HTTPS for the REST API." )
    public static final Setting<Boolean> webserver_https_enabled = setting( "org.neo4j.server.webserver.https.enabled",
            BOOLEAN, FALSE );

    @Description( "HTTPS port for the REST API." )
    public static final Setting<Integer> webserver_https_port = setting( "org.neo4j.server.webserver.https.port",
            INTEGER, "7473", port );

    @Description( "Path to the keystore used to store SSL certificates and keys while the server is running." )
    public static final Setting<File> webserver_keystore_path = setting(
            "org.neo4j.server.webserver.https.keystore.location", PATH, "neo4j-home/ssl/keystore" );

    @Description( "Path to the SSL certificate used for HTTPS connections." )
    public static final Setting<File> webserver_https_cert_path = setting(
            "org.neo4j.server.webserver.https.cert.location", PATH, "neo4j-home/ssl/snakeoil.cert" );

    @Description( "Path to the SSL key used for HTTPS connections." )
    public static final Setting<File> webserver_https_key_path = setting(
            "org.neo4j.server.webserver.https.key.location", PATH, "neo4j-home/ssl/snakeoil.key" );

    @Description( "Enable HTTP request logging." )
    public static final Setting<Boolean> http_logging_enabled = setting( "org.neo4j.server.http.log.enabled", BOOLEAN, FALSE );

    @Description( "Enable HTTP content logging." )
    public static final Setting<Boolean> http_content_logging_enabled = setting( "org.neo4j.server.http.unsafe.content_log.enabled", BOOLEAN, FALSE );

    @Description( "Path to a logback configuration file for HTTP request logging." )
    public static final Setting<File> http_log_config_File = setting( "org.neo4j.server.http.log.config", PATH,
            NO_DEFAULT );

    @Description( "Timeout for idle transactions." )
    public static final Setting<Long> transaction_timeout = setting( "org.neo4j.server.transaction.timeout", DURATION, "60s" );

    @Description( "Enable auth requirement to access Neo4j." )
    public static final Setting<Boolean> auth_enabled = setting("dbms.security.auth_enabled",
            BOOLEAN, TRUE);
}
