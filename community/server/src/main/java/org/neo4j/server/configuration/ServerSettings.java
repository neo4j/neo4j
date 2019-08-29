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
package org.neo4j.server.configuration;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.server.web.JettyThreadCalculator;

import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.NORMALIZED_RELATIVE_URI;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;

@ServiceProvider
public class ServerSettings implements SettingsDeclaration
{
    @Description( "Maximum request header size" )
    @Internal
    public static final Setting<Integer> maximum_request_header_size =
            newBuilder( "unsupported.dbms.max_http_request_header_size", INT, 20480 ).build();

    @Description( "Maximum response header size" )
    @Internal
    public static final Setting<Integer> maximum_response_header_size =
            newBuilder( "unsupported.dbms.max_http_response_header_size", INT, 20480 ).build();

    @Description( "Number of Neo4j worker threads. This setting is only valid for REST, and does not influence bolt-server. " +
            "It sets the amount of worker threads for the Jetty server used by neo4j-server. " +
            "This option can be tuned when you plan to execute multiple, concurrent REST requests, " +
            "with the aim of getting more throughput from the database. " +
            "Your OS might enforce a lower limit than the maximum value specified here." )
    @DocumentedDefaultValue( "Number of available processors, or 500 for machines which have more than 500 processors." )
    public static final Setting<Integer> webserver_max_threads =
            newBuilder( "dbms.threads.worker_count", INT, Math.min( Runtime.getRuntime().availableProcessors(), 500 ) )
                    .addConstraint( range( 1, JettyThreadCalculator.MAX_THREADS ) ).build();

    @Description( "If execution time limiting is enabled in the database, this configures the maximum request execution time. " +
            "Please use dbms.transaction.timeout instead." )
    @Internal
    @Deprecated
    public static final Setting<Duration> webserver_limit_execution_time =
            newBuilder( "unsupported.dbms.executiontime_limit.time", DURATION, null ).build();

    private static final SettingValueParser<ThirdPartyJaxRsPackage> MOUNT_POINTS = new SettingValueParser<>()
    {
        @Override
        public ThirdPartyJaxRsPackage parse( String packageAndMountpoint )
        {
            String[] parts = packageAndMountpoint.split( "=" );
            if ( parts.length != 2 )
            {
                throw new IllegalArgumentException( "config for " + ServerSettings.third_party_packages.name() + " is wrong: " + packageAndMountpoint );
            }
            String pkg = parts[0];
            String mountPoint = parts[1];
            return new ThirdPartyJaxRsPackage( pkg, mountPoint );
        }

        @Override
        public String getDescription()
        {
            return "<classname>=<mount point> string";
        }

        @Override
        public Class<ThirdPartyJaxRsPackage> getType()
        {
            return ThirdPartyJaxRsPackage.class;
        }
    };

    @Description( "Comma-separated list of <classname>=<mount point> for unmanaged extensions." )
    public static final Setting<List<ThirdPartyJaxRsPackage>> third_party_packages =
            newBuilder( "dbms.unmanaged_extension_classes", listOf( MOUNT_POINTS ), emptyList() ).build();

    @Description( "Value of the Access-Control-Allow-Origin header sent over any HTTP or HTTPS " +
            "connector. This defaults to '*', which allows broadest compatibility. Note " +
            "that any URI provided here limits HTTP/HTTPS access to that URI only." )
    public static final Setting<String> http_access_control_allow_origin =
            newBuilder( "dbms.security.http_access_control_allow_origin", STRING, "*" ).build();

    @Description( "Enable HTTP request logging." )
    public static final Setting<Boolean> http_logging_enabled = newBuilder( "dbms.logs.http.enabled", BOOL, false ).build();

    @Description( "Path to HTTP request log." )
    public static final Setting<Path> http_log_path =
            newBuilder( "dbms.logs.http.path", PATH, Path.of( "http.log" ) ).setDependency( logs_directory ).immutable().build();

    @Description( "Number of HTTP logs to keep." )
    public static final Setting<Integer> http_logging_rotation_keep_number =
            newBuilder( "dbms.logs.http.rotation.keep_number", INT, 5 ).build();

    @Description( "Size of each HTTP log that is kept." )
    public static final Setting<Long> http_logging_rotation_size =
            newBuilder( "dbms.logs.http.rotation.size", BYTES, ByteUnit.mebiBytes( 20 ) ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Timeout for idle transactions in the REST endpoint." )
    public static final Setting<Duration> transaction_idle_timeout =
            newBuilder( "dbms.rest.transaction.idle_timeout", DURATION, Duration.ofSeconds( 60 ) ).build();

    @Description( "Value of the HTTP Strict-Transport-Security (HSTS) response header. " +
            "This header tells browsers that a webpage should only be accessed using HTTPS instead of HTTP. It is attached to every HTTPS response. " +
            "Setting is not set by default so 'Strict-Transport-Security' header is not sent. " +
            "Value is expected to contain directives like 'max-age', 'includeSubDomains' and 'preload'." )
    public static final Setting<String> http_strict_transport_security = newBuilder( "dbms.security.http_strict_transport_security", STRING, null ).build();

    @Description( "Defines a whitelist of http paths where Neo4j authentication is not required." )
    public static final Setting<List<String>> http_auth_whitelist =
            newBuilder( "dbms.security.http_auth_whitelist", listOf( STRING ), List.of( "/", "/browser.*" ) ).build();

    @Description( "Defines a blacklist of http paths that should not be accessed." )
    public static final Setting<List<String>> http_paths_blacklist =
            newBuilder( "dbms.security.http_paths_blacklist", listOf( STRING ), emptyList() ).build();

    @Internal
    @Description( "Publicly discoverable bolt:// URI to use for Neo4j Drivers wanting to access the data in this " +
            "particular database instance. Normally this is the same as the advertised address configured for the " +
            "connector, but this allows manually overriding that default." )
    @DocumentedDefaultValue( "Defaults to a bolt://-schemed version of the advertised address of the first found bolt connector." )
    public static final Setting<URI> bolt_discoverable_address =
            newBuilder( "unsupported.dbms.discoverable_bolt_address", SettingValueParsers.URI, SettingValueParsers.URI.parse( "" ) ).build();

    @Internal
    @Description( "Publicly discoverable neo4j:// URI to use for Neo4j Drivers wanting to access a cluster or a single instance." )
    @DocumentedDefaultValue( "Defaults to empty on any deployment that is not a causal cluster core, and a " +
            "neo4j://-schemed URI of the advertised address of the bolt connector." )
    public static final Setting<URI> bolt_routing_discoverable_address =
            newBuilder( "unsupported.dbms.discoverable_bolt_routing_address", SettingValueParsers.URI, SettingValueParsers.URI.parse( "" ) ).build();

    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Commands to be run when Neo4j Browser successfully connects to this server. Separate multiple " +
            "commands with semi-colon." )
    public static final Setting<String> browser_postConnectCmd = newBuilder( "browser.post_connect_cmd", STRING, "" ).build();

    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Whitelist of hosts for the Neo4j Browser to be allowed to fetch content from." )
    public static final Setting<String> browser_remoteContentHostnameWhitelist =
            newBuilder( "browser.remote_content_hostname_whitelist", STRING, "guides.neo4j.com,localhost").build();

    @Internal
    @Description( "The legacy data endpoint. This is kept for back-compatibility purpose." )
    public static final Setting<URI> rest_api_path =
            newBuilder( "unsupported.dbms.uris.rest", NORMALIZED_RELATIVE_URI, NORMALIZED_RELATIVE_URI.parse( "/db/data" ) ).build();

    @Internal
    @Description( "The legacy manage endpoint. This is kept for back-compatibility purpose." )
    public static final Setting<URI> management_api_path =
            newBuilder( "unsupported.dbms.uris.management", NORMALIZED_RELATIVE_URI, NORMALIZED_RELATIVE_URI.parse( "/db/manage" ) ).build();

    @Internal
    @Description( "The start endpoint of database api." )
    public static final Setting<URI> db_api_path =
            newBuilder( "unsupported.dbms.uris.db", NORMALIZED_RELATIVE_URI, NORMALIZED_RELATIVE_URI.parse( "/db" ) ).build();

    @Internal
    public static final Setting<URI> browser_path =
            newBuilder( "unsupported.dbms.uris.browser", SettingValueParsers.URI, SettingValueParsers.URI.parse( "/browser/" ) ).build();

    @Internal
    public static final Setting<Boolean> wadl_enabled =
            newBuilder( "unsupported.dbms.wadl_generation_enabled", BOOL, false ).build();
}
