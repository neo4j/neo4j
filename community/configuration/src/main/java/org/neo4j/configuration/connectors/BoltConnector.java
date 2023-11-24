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
package org.neo4j.configuration.connectors;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.SettingConstraints.NO_ALL_INTERFACES_ADDRESS;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;

import java.time.Duration;
import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;

@ServiceProvider
@PublicApi
public final class BoltConnector implements SettingsDeclaration {
    public static final int DEFAULT_PORT = 7687;

    public static final String NAME = "bolt";
    public static final String INTERNAL_NAME = "bolt-internal";

    @Description("Enable the bolt connector")
    @DocumentedDefaultValue("true") // Should document server defaults.
    public static final Setting<Boolean> enabled = ConnectorDefaults.bolt_enabled;

    @Description("Enable the collection of driver telemetry.")
    @DocumentedDefaultValue("false")
    public static final Setting<Boolean> server_bolt_telemetry_enabled =
            newBuilder("server.bolt.telemetry.enabled", BOOL, false).build();

    @Description("Encryption level to require this connector to use")
    public static final Setting<EncryptionLevel> encryption_level = newBuilder(
                    "server.bolt.tls_level", ofEnum(EncryptionLevel.class), DISABLED)
            .build();

    @Description("Address the connector should bind to")
    public static final Setting<SocketAddress> listen_address = newBuilder(
                    "server.bolt.listen_address", SOCKET_ADDRESS, new SocketAddress(DEFAULT_PORT))
            .setDependency(default_listen_address)
            .build();

    @Description("Advertised address for this connector")
    public static final Setting<SocketAddress> advertised_address = newBuilder(
                    "server.bolt.advertised_address", SOCKET_ADDRESS, new SocketAddress(DEFAULT_PORT))
            .addConstraint(NO_ALL_INTERFACES_ADDRESS)
            .setDependency(default_advertised_address)
            .build();

    @DocumentedDefaultValue("STREAMING")
    @Description("The type of messages to enable keep-alive messages for (ALL, STREAMING or OFF)")
    public static final Setting<KeepAliveRequestType> connection_keep_alive_type = newBuilder(
                    "server.bolt.connection_keep_alive_for_requests",
                    ofEnum(KeepAliveRequestType.class),
                    KeepAliveRequestType.ALL)
            .build();

    @DocumentedDefaultValue("1m")
    @Description(
            "The maximum time to wait before sending a NOOP on connections waiting for responses from active ongoing queries. "
                    + "The minimum value is 1 millisecond.")
    public static final Setting<Duration> connection_keep_alive = newBuilder(
                    "server.bolt.connection_keep_alive", DURATION, ofMinutes(1))
            .addConstraint(min(ofMillis(1)))
            .build();

    @DocumentedDefaultValue("1m")
    @Description("The interval between every scheduled keep-alive check on all connections with active queries. "
            + "Zero duration turns off keep-alive service.")
    public static final Setting<Duration> connection_keep_alive_streaming_scheduling_interval = newBuilder(
                    "server.bolt.connection_keep_alive_streaming_scheduling_interval", DURATION, ofMinutes(1))
            .addConstraint(min(ofSeconds(0)))
            .build();

    @DocumentedDefaultValue("2")
    @Description("The total amount of probes to be missed before a connection is considered stale."
            + "The minimum for this value is 1.")
    public static final Setting<Integer> connection_keep_alive_probes = newBuilder(
                    "server.bolt.connection_keep_alive_probes", INT, 2)
            .addConstraint(min(1))
            .build();

    @Description("The number of threads to keep in the thread pool bound to this connector, even if they are idle.")
    public static final Setting<Integer> thread_pool_min_size =
            newBuilder("server.bolt.thread_pool_min_size", INT, 5).build();

    @Description("The maximum number of threads allowed in the thread pool bound to this connector.")
    public static final Setting<Integer> thread_pool_max_size =
            newBuilder("server.bolt.thread_pool_max_size", INT, 400).build();

    @Description("The maximum time an idle thread in the thread pool bound to this connector will wait for new tasks.")
    public static final Setting<Duration> thread_pool_keep_alive = newBuilder(
                    "server.bolt.thread_pool_keep_alive", DURATION, ofMinutes(5))
            .build();

    public enum EncryptionLevel {
        REQUIRED,
        OPTIONAL,
        DISABLED
    }

    public enum KeepAliveRequestType {

        /**
         * Causes keep-alive messages to be sent while the server is computing a response to a given driver command.
         */
        ALL,

        /**
         * Causes keep-alive messages to be sent only while streaming results.
         */
        @Deprecated(forRemoval = true)
        STREAMING,

        /**
         * Disables keep-alive messages entirely.
         */
        OFF
    }
}
