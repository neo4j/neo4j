/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.configuration;

import static java.util.Collections.emptyList;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.NORMALIZED_RELATIVE_URI;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.setOfEnums;

import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.web.JettyThreadCalculator;

@ServiceProvider
public class ServerSettings implements SettingsDeclaration {
    public static final String DBMS_MOUNT_POINT = "/dbms";

    @Description("Maximum request header size")
    @Internal
    public static final Setting<Integer> maximum_request_header_size =
            newBuilder("internal.dbms.max_http_request_header_size", INT, 20480).build();

    @Description("Maximum response header size")
    @Internal
    public static final Setting<Integer> maximum_response_header_size = newBuilder(
                    "internal.dbms.max_http_response_header_size", INT, 20480)
            .build();

    @Description(
            "Number of Neo4j worker threads. This setting is only valid for REST, and does not influence bolt-server. "
                    + "It sets the amount of worker threads for the Jetty server used by neo4j-server. "
                    + "This option can be tuned when you plan to execute multiple, concurrent REST requests, "
                    + "with the aim of getting more throughput from the database. "
                    + "By default, it is set to the number of available processors, or to 500 for machines "
                    + "with more than 500 processors. "
                    + "Your OS might enforce a lower limit than the maximum value specified here.")
    public static final Setting<Integer> webserver_max_threads = newBuilder(
                    "server.threads.worker_count",
                    INT,
                    Math.min(Runtime.getRuntime().availableProcessors(), 500))
            .addConstraint(range(1, JettyThreadCalculator.MAX_THREADS))
            .build();

    @Description(
            "If execution time limiting is enabled in the database, this configures the maximum request execution time. "
                    + "Please use db.transaction.timeout instead.")
    @Internal
    @Deprecated
    public static final Setting<Duration> webserver_limit_execution_time =
            newBuilder("internal.dbms.executiontime_limit.time", DURATION, null).build();

    private static final SettingValueParser<ThirdPartyJaxRsPackage> MOUNT_POINTS = new SettingValueParser<>() {
        @Override
        public ThirdPartyJaxRsPackage parse(String packageAndMountpoint) {
            String[] parts = packageAndMountpoint.split("=");
            if (parts.length != 2) {
                throw new IllegalArgumentException("config for " + ServerSettings.third_party_packages.name()
                        + " is wrong: " + packageAndMountpoint);
            }
            String pkg = parts[0];
            String mountPoint = parts[1];
            return new ThirdPartyJaxRsPackage(pkg, mountPoint);
        }

        @Override
        public String getDescription() {
            return "<classname>=<mount point> string";
        }

        @Override
        public Class<ThirdPartyJaxRsPackage> getType() {
            return ThirdPartyJaxRsPackage.class;
        }
    };

    @Description("Comma-separated list of <classname>=<mount point> for unmanaged extensions.")
    public static final Setting<List<ThirdPartyJaxRsPackage>> third_party_packages = newBuilder(
                    "server.unmanaged_extension_classes", listOf(MOUNT_POINTS), emptyList())
            .build();

    @Description("Value of the Access-Control-Allow-Origin header sent over any HTTP or HTTPS "
            + "connector. This defaults to '*', which allows broadest compatibility. Note "
            + "that any URI provided here limits HTTP/HTTPS access to that URI only.")
    public static final Setting<String> http_access_control_allow_origin = newBuilder(
                    "dbms.security.http_access_control_allow_origin", STRING, "*")
            .build();

    @Description("Enable HTTP request logging.")
    public static final Setting<Boolean> http_logging_enabled =
            newBuilder("dbms.logs.http.enabled", BOOL, false).build();

    @Description("Value of the HTTP Strict-Transport-Security (HSTS) response header. "
            + "This header tells browsers that a webpage should only be accessed using HTTPS instead of HTTP. It is attached to every HTTPS response. "
            + "Setting is not set by default so 'Strict-Transport-Security' header is not sent. "
            + "Value is expected to contain directives like 'max-age', 'includeSubDomains' and 'preload'.")
    public static final Setting<String> http_strict_transport_security = newBuilder(
                    "dbms.security.http_strict_transport_security", STRING, null)
            .build();

    @Description("Defines an allowlist of http paths where Neo4j authentication is not required.")
    public static final Setting<List<String>> http_auth_allowlist = newBuilder(
                    "dbms.security.http_auth_allowlist", listOf(STRING), List.of("/", "/browser.*"))
            .build();

    @Internal
    @Description("Defines a blacklist of http paths that should not be accessed.")
    public static final Setting<List<String>> http_paths_blacklist = newBuilder(
                    "internal.dbms.http_paths_blacklist", listOf(STRING), emptyList())
            .build();

    @Description(
            "Defines the set of modules loaded into the Neo4j web server. The enterprise management endpoints are only available in the enterprise edition.")
    public static final Setting<Set<ConfigurableServerModules>> http_enabled_modules = newBuilder(
                    "server.http_enabled_modules",
                    setOfEnums(ConfigurableServerModules.class),
                    EnumSet.complementOf(EnumSet.of(ConfigurableServerModules.QUERY_API_ENDPOINTS)))
            .build();

    @Description("Defines the set of transports available on the HTTP server")
    public static final Setting<Set<ConfigurableTransports>> http_enabled_transports = newBuilder(
                    "server.http_enabled_transports",
                    setOfEnums(ConfigurableTransports.class),
                    EnumSet.allOf(ConfigurableTransports.class))
            .build();

    @Internal
    @Description("Publicly discoverable bolt:// URI to use for Neo4j Drivers wanting to access the data in this "
            + "particular database instance. Normally this is the same as the advertised address configured for the "
            + "connector, but this allows manually overriding that default. "
            + "Defaults to a bolt://-schemed version of the advertised address of the first found bolt connector.")
    public static final Setting<URI> bolt_discoverable_address = newBuilder(
                    "internal.dbms.discoverable_bolt_address",
                    SettingValueParsers.URI,
                    SettingValueParsers.URI.parse(""))
            .build();

    @Internal
    @Description(
            "Publicly discoverable neo4j:// URI to use for Neo4j Drivers wanting to access a cluster or a single instance. "
                    + "Defaults to empty on any deployment that is not a cluster core, and a "
                    + "neo4j://-schemed URI of the advertised address of the bolt connector.")
    public static final Setting<URI> bolt_routing_discoverable_address = newBuilder(
                    "internal.dbms.discoverable_bolt_routing_address",
                    SettingValueParsers.URI,
                    SettingValueParsers.URI.parse(""))
            .build();

    @SuppressWarnings("unused") // accessed from the browser
    @Description("Commands to be run when Neo4j Browser successfully connects to this server. Separate multiple "
            + "commands with semi-colon.")
    public static final Setting<String> browser_post_connect_cmd =
            newBuilder("browser.post_connect_cmd", STRING, "").build();

    @SuppressWarnings("unused") // accessed from the browser
    @Description("Whitelist of hosts for the Neo4j Browser to be allowed to fetch content from.")
    public static final Setting<String> browser_remote_content_hostname_whitelist = newBuilder(
                    "browser.remote_content_hostname_whitelist", STRING, "guides.neo4j.com,localhost")
            .build();

    @SuppressWarnings("unused") // accessed from browser and other graph apps
    @Description("Configure client applications such as Browser and Bloom to send Product Analytics data.")
    public static final Setting<Boolean> allow_telemetry =
            newBuilder("client.allow_telemetry", BOOL, true).build();

    @Internal
    @Description("The legacy manage endpoint. This is kept for back-compatibility purpose.")
    public static final Setting<URI> management_api_path = newBuilder(
                    "internal.dbms.uris.management",
                    NORMALIZED_RELATIVE_URI,
                    NORMALIZED_RELATIVE_URI.parse("/db/manage"))
            .build();

    @Internal
    @Description("The start endpoint of database api.")
    public static final Setting<URI> db_api_path = newBuilder(
                    "internal.dbms.uris.db", NORMALIZED_RELATIVE_URI, NORMALIZED_RELATIVE_URI.parse("/db"))
            .build();

    @Internal
    @Description("The start endpoint of the dbms api.")
    public static final Setting<URI> dbms_api_path = newBuilder(
                    "internal.dbms.uris.dbms", NORMALIZED_RELATIVE_URI, NORMALIZED_RELATIVE_URI.parse("/dbms"))
            .build();

    @Internal
    @Description("URI to the browser home page")
    public static final Setting<URI> browser_path = newBuilder(
                    "internal.dbms.uris.browser", SettingValueParsers.URI, SettingValueParsers.URI.parse("/browser/"))
            .build();

    @Internal
    @Description("Toggle WADL generation. Matching the underlying jersey server config")
    public static final Setting<Boolean> wadl_enabled =
            newBuilder("internal.dbms.wadl_generation_enabled", BOOL, false).build();

    @Internal
    @Description("Enable Clacks module")
    public static final Setting<Boolean> clacks_enabled =
            newBuilder("internal.dbms.clacks_enabled", BOOL, false).build();

    @Internal
    @Description("Clacks module names")
    public static final Setting<String> clacks_names = newBuilder(
                    "internal.dbms.clacks_names", STRING, "Richard Macaskill")
            .build();
}
