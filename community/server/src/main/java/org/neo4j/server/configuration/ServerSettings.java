/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.configuration.Settings;

import static org.neo4j.kernel.configuration.Settings.*;

@Description("Settings used by the server configuration")
public interface ServerSettings
{
    /**
     * Key for the configuration file system property.
     */
    String SERVER_CONFIG_FILE_KEY = "org.neo4j.config.file";

    /**
     * Default path for the configuration file. The path should always be get/set using System.property.
     */
    String SERVER_CONFIG_FILE = "conf/neo4j.conf";

    @Description("Maximum request header size")
    @Internal
    Setting<Integer> maximum_request_header_size =
            setting( "unsupported.dbms.max_http_request_header_size", INTEGER, "20480" );

    @Description("Maximum response header size")
    @Internal
    Setting<Integer> maximum_response_header_size =
            setting( "unsupported.dbms.max_http_response_header_size", INTEGER, "20480" );

    @Description("Comma-seperated list of custom security rules for Neo4j to use.")
    Setting<List<String>> security_rules = setting( "dbms.security.http_authorization_classes", STRING_LIST, EMPTY );

    @Description("Http port for the Neo4j REST API.")
    Setting<Integer> webserver_port = setting( "org.neo4j.server.webserver.port", INTEGER, "7474", port );

    @Description("Hostname for the Neo4j REST API")
    Setting<String> webserver_address = BoltKernelExtension.Settings.webserver_address;

    @Description("Number of Neo4j worker threads.")
    Setting<Integer> webserver_max_threads = setting( "dbms.threads.worker_count",
            INTEGER, "" + Math.min( Runtime.getRuntime().availableProcessors(), 500 ), min( 1 ) );

    @Description("If execution time limiting is enabled in the database, this configures the maximum request execution time.")
    @Internal
    Setting<Long> webserver_limit_execution_time =
            setting( "unsupported.dbms.executiontime_limit.time", DURATION, NO_DEFAULT );

    @Internal
    Setting<List<String>> console_module_engines = setting(
            "unsupported.dbms.console_module.engines", STRING_LIST, "SHELL" );

    @Description("Comma-separated list of <classname>=<mount point> for unmanaged extensions.")
    Setting<List<ThirdPartyJaxRsPackage>> third_party_packages = setting( "dbms.unmanaged_extension_classes",
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

    @Description("Enable HTTPS for the REST API.")
    Setting<Boolean> webserver_https_enabled = setting( "org.neo4j.server.webserver.https.enabled", BOOLEAN, FALSE );

    @Description("HTTPS port for the REST API.")
    Setting<Integer> webserver_https_port = setting( "org.neo4j.server.webserver.https.port", INTEGER, "7473", port );

    @Description("Path to the X.509 public certificate to be used by Neo4j for TLS connections")
    Setting<File> tls_certificate_file = BoltKernelExtension.Settings.tls_certificate_file;

    @Description("Path to the X.509 private key to be used by Neo4j for TLS connections")
    Setting<File> tls_key_file = BoltKernelExtension.Settings.tls_key_file;

    @Description("Enable HTTP request logging.")
    Setting<Boolean> http_logging_enabled = setting( "org.neo4j.server.http.log.enabled", BOOLEAN, FALSE );

    @Description("Enable HTTP content logging.")
    Setting<Boolean> http_content_logging_enabled = setting( "org.neo4j.server.http.unsafe.content_log.enabled",
            BOOLEAN, FALSE );

    @Description("Path to a logback configuration file for HTTP request logging.")
    Setting<File> http_log_config_file = setting( "org.neo4j.server.http.log.config", new HttpLogSetting(),
            NO_DEFAULT );

    @Description("Enable GC Logging")
    Setting<Boolean> gc_logging_enabled = setting("dbms.logs.gc.enabled", BOOLEAN, FALSE);

    @Description("GC Logging Options")
    Setting<String> gc_logging_options = setting("dbms.logs.gc.options", STRING, "" +
            "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime " +
            "-XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution");

    @Description("Number of GC logs to keep.")
    Setting<Integer> gc_logging_rotation_keep_number = setting("dbms.logs.gc.rotation.keep_number", INTEGER, "5");

    @Description("Size of each GC log that is kept.")
    Setting<Long> gc_logging_rotation_size = setting("dbms.logs.rotation.gc.size", BYTES, "20m", min(0L), max( Long.MAX_VALUE ) );

    @Description("Path of the run directory")
    Setting<File> run_directory = setting("dbms.directories.run", PATH, "run");

    @Description("Timeout for idle transactions.")
    Setting<Long> transaction_timeout = setting( "dbms.transaction_timeout", DURATION, "60s" );

    @Internal
    Setting<URI> rest_api_path = setting( "unsupported.dbms.uris.rest", NORMALIZED_RELATIVE_URI, "/db/data" );

    @Internal
    Setting<URI> management_api_path = setting( "unsupported.dbms.uris.management",
            NORMALIZED_RELATIVE_URI, "/db/manage" );

    @Internal
    Setting<URI> browser_path = setting( "unsupported.dbms.uris.browser", Settings.URI, "/browser/" );

    @Internal
    Setting<Boolean> script_sandboxing_enabled = setting("unsupported.dbms.security.script_sandboxing_enabled",
            BOOLEAN, TRUE );

    @Internal
    Setting<Boolean> wadl_enabled = setting( "unsupported.dbms.wadl_generation_enabled", BOOLEAN,
            FALSE );

    @Internal
    Setting<Boolean> console_module_enabled = setting( "unsupported.dbms.console_module.enabled", BOOLEAN, TRUE );
}
