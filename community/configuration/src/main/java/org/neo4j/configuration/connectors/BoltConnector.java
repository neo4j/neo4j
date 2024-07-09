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
import static org.neo4j.configuration.SettingConstraints.any;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.configuration.SettingValueParsers.setOf;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;

import java.time.Duration;
import java.util.Set;
import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingConstraints;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;

@ServiceProvider
@PublicApi
public final class BoltConnector implements SettingsDeclaration {
    public static final int DEFAULT_PORT = 7687;

    public static final String NAME = "bolt";
    public static final String INTERNAL_NAME = "bolt-internal";

    @Description("Enable the Bolt connector.")
    public static final Setting<Boolean> enabled = ConnectorDefaults.bolt_enabled;

    @Description("Enable the collection of driver telemetry.")
    public static final Setting<Boolean> server_bolt_telemetry_enabled =
            newBuilder("server.bolt.telemetry.enabled", BOOL, false).build();

    @Description("The encryption level to be used to secure communications with this connector.")
    public static final Setting<EncryptionLevel> encryption_level = newBuilder(
                    "server.bolt.tls_level", ofEnum(EncryptionLevel.class), DISABLED)
            .build();

    @Description("Address the connector should bind to.")
    public static final Setting<SocketAddress> listen_address = newBuilder(
                    "server.bolt.listen_address", SOCKET_ADDRESS, new SocketAddress(DEFAULT_PORT))
            .setDependency(default_listen_address)
            .build();

    @Description("Additional addresses the connector should bind to.")
    public static final Setting<Set<SocketAddress>> additional_listen_addresses = newBuilder(
                    "server.bolt.additional_listen_addresses", setOf(SOCKET_ADDRESS), Set.of())
            .build();

    @Description("Advertised address for this connector.")
    public static final Setting<SocketAddress> advertised_address = newBuilder(
                    "server.bolt.advertised_address", SOCKET_ADDRESS, new SocketAddress(DEFAULT_PORT))
            .addConstraint(NO_ALL_INTERFACES_ADDRESS)
            .setDependency(default_advertised_address)
            .build();

    @Description("The type of messages to enable keep-alive messages for `ALL`, `STREAMING`, or `OFF`.")
    public static final Setting<KeepAliveRequestType> connection_keep_alive_type = newBuilder(
                    "server.bolt.connection_keep_alive_for_requests",
                    ofEnum(KeepAliveRequestType.class),
                    KeepAliveRequestType.ALL)
            .build();

    @Description(
            "The maximum time to wait before sending a NOOP on connections waiting for responses from active ongoing queries. "
                    + "The minimum value is 1 millisecond.")
    public static final Setting<Duration> connection_keep_alive = newBuilder(
                    "server.bolt.connection_keep_alive", DURATION, ofMinutes(1))
            .addConstraint(min(ofMillis(1)))
            .build();

    @Description("The interval between every scheduled keep-alive check on all connections with active queries. "
            + "Zero duration turns off keep-alive service.")
    public static final Setting<Duration> connection_keep_alive_streaming_scheduling_interval = newBuilder(
                    "server.bolt.connection_keep_alive_streaming_scheduling_interval", DURATION, ofMinutes(1))
            .addConstraint(min(ofSeconds(0)))
            .build();

    @Description("The total number of probes to be missed before a connection is considered stale. "
            + "The minimum value is 1.")
    public static final Setting<Integer> connection_keep_alive_probes = newBuilder(
                    "server.bolt.connection_keep_alive_probes", INT, 2)
            .addConstraint(min(1))
            .build();

    @Description("The number of threads, including idle, to keep in the thread pool bound to this connector.")
    public static final Setting<Integer> thread_pool_min_size =
            newBuilder("server.bolt.thread_pool_min_size", INT, 5).build();

    @Description("The maximum number of threads allowed in the thread pool bound to this connector.")
    public static final Setting<Integer> thread_pool_max_size =
            newBuilder("server.bolt.thread_pool_max_size", INT, 400).build();

    @Description("The maximum time an idle thread in the thread pool bound to this connector waits for new tasks.")
    public static final Setting<Duration> thread_pool_keep_alive = newBuilder(
                    "server.bolt.thread_pool_keep_alive", DURATION, ofMinutes(5))
            .build();

    @Description(
            "Enables accounting-based reporting of benign errors within the Bolt stack. When enabled, benign errors are reported only when such events occur with unusual frequency. When disabled, all benign network errors are reported.")
    public static final Setting<Boolean> enable_error_accounting = newBuilder(
                    "server.bolt.enable_network_error_accounting", BOOL, true)
            .build();

    @Description(
            "The maximum number of network-related connection aborts allowed within a specified time window before emitting log messages. A value of zero reverts to legacy warning behavior.")
    public static final Setting<Long> network_abort_warn_threshold = newBuilder(
                    "server.bolt.network_abort_warn_threshold", LONG, 2L)
            .addConstraint(min(1L))
            .build();

    @Description("The duration of the window in which network-related connection aborts are sampled.")
    public static final Setting<Duration> network_abort_warn_window_duration = newBuilder(
                    "server.bolt.network_abort_warn_window_duration", DURATION, ofMinutes(10))
            .addConstraint(min(ofSeconds(1)))
            .build();

    @Description(
            "The duration for which network-related connection aborts need to remain at a reasonable level before the error is cleared.")
    public static final Setting<Duration> network_abort_clear_window_duration = newBuilder(
                    "server.bolt.network_abort_clear_window_duration", DURATION, ofMinutes(10))
            .addConstraint(min(ofSeconds(1)))
            .build();

    @Description(
            "The maximum number of unscheduled requests allowed during thread starvation events within a specified time window before emitting log messages.")
    public static final Setting<Long> thread_starvation_warn_threshold = newBuilder(
                    "server.bolt.thread_starvation_warn_threshold", LONG, 2L)
            .addConstraint(min(1L))
            .build();

    @Description("The duration of the window in which unscheduled requests are sampled.")
    public static final Setting<Duration> thread_starvation_warn_window_duration = newBuilder(
                    "server.bolt.thread_starvation_warn_window_duration", DURATION, ofMinutes(10))
            .addConstraint(min(ofSeconds(1)))
            .build();

    @Description(
            "The duration for which unscheduled requests need to remain at a reasonable level before the error is cleared.")
    public static final Setting<Duration> thread_starvation_clear_window_duration = newBuilder(
                    "server.bolt.thread_starvation_clear_window_duration", DURATION, ofMinutes(10))
            .addConstraint(min(ofSeconds(1)))
            .build();

    @Description(
            "Amount of time spent between samples of current traffic usage. Lower values result in more accurate reporting while incurring a higher performance penalty. A value of zero disables traffic accounting.")
    public static final Setting<Duration> traffic_accounting_check_period = newBuilder(
                    "server.bolt.traffic_accounting_check_period", DURATION, ofMinutes(5))
            .addConstraint(any(SettingConstraints.is(Duration.ZERO), min(ofMinutes(1))))
            .build();

    @Description("Time to be spent below the configured traffic threshold to clear traffic warnings.")
    public static final Setting<Duration> traffic_accounting_clear_duration = newBuilder(
                    "server.bolt.traffic_accounting_clear_duration", DURATION, ofMinutes(10))
            .addConstraint(min(ofMinutes(1)))
            .build();

    @Description(
            "Maximum permitted incoming traffic within a configured accounting check window before emitting a warning (in Mbps).")
    public static final Setting<Long> traffic_accounting_incoming_threshold_mbps = newBuilder(
                    "server.bolt.traffic_accounting_incoming_threshold_mbps", LONG, 950L)
            .addConstraint(min(1L))
            .build();

    @Description(
            "Maximum permitted outgoing traffic within a configured accounting check window before emitting a warning (in Mbps).")
    public static final Setting<Long> traffic_accounting_outgoing_threshold_mbps = newBuilder(
                    "server.bolt.traffic_accounting_outgoing_threshold_mbps", LONG, 950L)
            .addConstraint(min(1L))
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
