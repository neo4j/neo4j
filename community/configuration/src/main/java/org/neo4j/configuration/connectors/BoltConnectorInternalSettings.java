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

import static java.lang.String.format;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.SettingConstraints.any;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.minSize;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.CIDR_IP;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.io.ByteUnit.kibiBytes;

import inet.ipaddr.IPAddressString;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;

@ServiceProvider
public final class BoltConnectorInternalSettings implements SettingsDeclaration {

    public static final String LOOPBACK_NAME = "bolt-loopback";
    public static final String LOCAL_NAME = "bolt-local";
    public static final SettingValueParser<ConfiguredProtocolVersion> PROTOCOL_VERSION = new SettingValueParser<>() {
        @Override
        public ConfiguredProtocolVersion parse(String value) {
            return ConfiguredProtocolVersion.fromString(value);
        }

        @Override
        public String getDescription() {
            return "a protocol version (<major>.<minor>)";
        }

        @Override
        public Class<ConfiguredProtocolVersion> getType() {
            return ConfiguredProtocolVersion.class;
        }
    };

    @Internal
    @Description("Enable protocol level logging for incoming connections on the Bolt connector")
    public static final Setting<Boolean> protocol_logging =
            newBuilder("internal.server.bolt.protocol_logging", BOOL, false).build();

    @Internal
    @Description(
            "Selects the format in which protocol level log messages are formatted (RAW for wire format, DECODED for decoded server view, BOTH for both formats)")
    public static final Setting<ProtocolLoggingMode> protocol_logging_mode = newBuilder(
                    "internal.server.bolt.protocol_logging_mode",
                    SettingValueParsers.ofEnum(ProtocolLoggingMode.class),
                    ProtocolLoggingMode.DECODED)
            .build();

    @Internal
    @Description("Enable capture of traffic logging for incoming connections on the Bolt connector")
    public static final Setting<Boolean> protocol_capture =
            newBuilder("internal.server.bolt.protocol_capture", BOOL, false).build();

    @Internal
    @Description("Path of the data directory. You must not configure more than one Neo4j installation to use the "
            + "same data directory.")
    public static final Setting<Path> protocol_capture_path = newBuilder(
                    "internal.server.bolt.protocol_capture_path", PATH, Path.of("bolt_capture"))
            .setDependency(GraphDatabaseSettings.neo4j_home)
            .immutable()
            .build();

    @Internal
    @Description("Enable/disable the use of native transports for netty")
    public static final Setting<Boolean> use_native_transport = newBuilder(
                    "internal.dbms.bolt.netty_server_use_native_transport", BOOL, true)
            .build();

    @Internal
    @Description(
            "The queue size of the thread pool bound to this connector (-1 for unbounded, 0 for direct handoff, > 0 for bounded)")
    public static final Setting<Integer> unsupported_thread_pool_queue_size =
            newBuilder("internal.server.bolt.thread_pool_queue_size", INT, 0).build();

    @Internal
    @Description("Enable TCP keep alive probes on this connector")
    public static final Setting<Boolean> tcp_keep_alive =
            newBuilder("internal.server.bolt.tcp_keep_alive", BOOL, true).build();

    @Internal
    @Description("Enable TCP fast open on this connector")
    public static final Setting<Boolean> tcp_fast_open =
            newBuilder("internal.server.bolt.tcp_fast_open", BOOL, false).build();

    @Internal
    @Description("The maximum number of pending TCP fast open connections within this connector")
    public static final Setting<Integer> tcp_fast_open_max_pending_connections = newBuilder(
                    "internal.server.bolt.tcp_fast_open_max_pending_connections", INT, 128)
            .addConstraint(min(1))
            .build();

    @Internal
    @Description("The maximum time to wait for a user to finish authentication before closing the connection.")
    public static final Setting<Duration> unsupported_bolt_unauth_connection_timeout = newBuilder(
                    "internal.server.bolt.unauth_connection_timeout", DURATION, ofSeconds(30))
            .build();

    @Internal
    @Description("The maximum inbound message size in bytes are allowed before a connection is authenticated.")
    public static final Setting<Long> unsupported_bolt_unauth_connection_max_inbound_bytes = newBuilder(
                    "internal.server.bolt.unauth_max_inbound_bytes", BYTES, ByteUnit.kibiBytes(8))
            .build();

    @Internal
    @Description(
            "The maximum amount of elements within incoming Packstream messages permitted before a connection is authenticated (0 for disabled, > 0 for limited).")
    public static final Setting<Integer> bolt_unauth_connection_max_structure_elements = newBuilder(
                    "internal.server.bolt.unauth_max_structure_elements", INT, 64)
            .addConstraint(range(0, Integer.MAX_VALUE))
            .build();

    @Internal
    @Description(
            "The maximum amount of depth within incoming Packstream messages permitted before a connection is authenticated (0 for disabled, > 0 for limited).")
    public static final Setting<Integer> bolt_unauth_connection_max_structure_depth = newBuilder(
                    "internal.server.bolt.unauth_max_structure_depth", INT, 4)
            .addConstraint(range(0, Integer.MAX_VALUE))
            .build();

    @Internal
    @Description("The maximum time to wait for the thread pool to finish processing its pending jobs and shutdown")
    public static final Setting<Duration> thread_pool_shutdown_wait_time = newBuilder(
                    "internal.server.bolt.thread_pool_shutdown_wait_time", DURATION, ofMinutes(5))
            .build();

    @Internal
    @Description("The maximum time to wait for a connection to shut down before issuing a warning")
    public static final Setting<Duration> connection_shutdown_wait_time = newBuilder(
                    "internal.server.bolt.connection_shutdown_wait_time", DURATION, ofMinutes(5))
            .build();

    @Internal
    @Description("Whether to apply network level outbound network buffer based throttling")
    public static final Setting<Boolean> bolt_outbound_buffer_throttle = newBuilder(
                    "internal.dbms.bolt.outbound_buffer_throttle", BOOL, true)
            .dynamic()
            .build();

    @Internal
    @Description("When the size (in bytes) of outbound network buffers, used by bolt's network layer, "
            + "grows beyond this value bolt channel will advertise itself as unwritable and will block "
            + "related processing thread until it becomes writable again.")
    public static final Setting<Integer> bolt_outbound_buffer_throttle_high_water_mark = newBuilder(
                    "internal.dbms.bolt.outbound_buffer_throttle.high_watermark", INT, (int) kibiBytes(512))
            .addConstraint(range((int) kibiBytes(64), Integer.MAX_VALUE))
            .build();

    @Internal
    @Description("When the size (in bytes) of outbound network buffers, previously advertised as unwritable, "
            + "gets below this value bolt channel will re-advertise itself as writable and blocked processing "
            + "thread will resume execution.")
    public static final Setting<Integer> bolt_outbound_buffer_throttle_low_water_mark = newBuilder(
                    "internal.dbms.bolt.outbound_buffer_throttle.low_watermark", INT, (int) kibiBytes(128))
            .addConstraint(range((int) kibiBytes(16), Integer.MAX_VALUE))
            .build();

    @Internal
    @Description("When the total time outbound network buffer based throttle lock is held exceeds this value, "
            + "the corresponding bolt channel will be aborted. Setting "
            + "this to 0 will disable this behaviour.")
    public static final Setting<Duration> bolt_outbound_buffer_throttle_max_duration = newBuilder(
                    "internal.dbms.bolt.outbound_buffer_throttle.max_duration", DURATION, ofMinutes(15))
            .addConstraint(any(min(ofSeconds(30)), is(Duration.ZERO)))
            .build();

    @Internal
    @Description("When the number of queued inbound messages grows beyond this value, reading from underlying "
            + "channel will be paused (no more inbound messages will be available) until queued number of "
            + "messages drops below the configured low watermark value.")
    public static final Setting<Integer> bolt_inbound_message_throttle_high_water_mark = newBuilder(
                    "internal.dbms.bolt.inbound_message_throttle.high_watermark", INT, 300)
            .addConstraint(range(1, Integer.MAX_VALUE))
            .build();

    @Internal
    @Description("When the number of queued inbound messages, previously reached configured high watermark value, "
            + "drops below this value, reading from underlying channel will be enabled and any pending messages "
            + "will start queuing again.")
    public static final Setting<Integer> bolt_inbound_message_throttle_low_water_mark = newBuilder(
                    "internal.dbms.bolt.inbound_message_throttle.low_watermark", INT, 100)
            .addConstraint(range(1, Integer.MAX_VALUE))
            .build();

    @Internal
    @Description("Enable/disable the use of a merge cumulator for netty")
    public static final Setting<Boolean> netty_message_merge_cumulator = newBuilder(
                    "internal.dbms.bolt.netty_message_merge_cumulator", BOOL, false)
            .build();

    @Internal
    @Description("Enable/disable PROXY protocol support (HAProxy v1/v2). "
            + "When enabled, the Bolt connector will automatically detect and decode PROXY protocol headers "
            + "from load balancers (e.g., HAProxy, AWS NLB with proxy protocol) that provide the real client IP address. "
            + "The connector gracefully handles both connections with and without PROXY protocol headers, "
            + "making it safe to enable even in mixed environments. "
            + "The real client address will be used for authentication, logging, connection tracking, "
            + "and security auditing instead of the proxy's address.")
    public static final Setting<Boolean> proxy_protocol_enabled =
            newBuilder("internal.dbms.bolt.proxy_protocol.enabled", BOOL, false).build();

    @Internal
    @Description("Enable/disable generation of response metrics")
    public static final Setting<Boolean> enable_response_metrics =
            newBuilder("internal.server.bolt.response_metrics", BOOL, false).build();

    @Internal
    @Description("Specifies the initial number of bytes requested when streaming records.")
    public static final Setting<Integer> streaming_buffer_size = newBuilder(
                    "internal.dbms.bolt.streaming_buffer_size", INT, 512)
            .addConstraint(min(128))
            .build();

    @Internal
    @Description("Specifies the minimum number of bytes which need to be written in order to flush the local network"
            + "pipelines thus making prior written records visible to clients.")
    public static final Setting<Integer> streaming_flush_threshold = newBuilder(
                    "internal.dbms.bolt.streaming_flush_threshold", INT, 8192)
            .addConstraint(any(is(0), min(128)))
            .build();

    @Internal
    @Description("Specifies whether transactions shall bind to a single thread for the duration of their lifetime")
    public static final Setting<Boolean> transaction_thread_binding = newBuilder(
                    "internal.dbms.bolt.transaction_thread_binding", BOOL, true)
            .build();

    @Internal
    @Description("Specifies the duration for which threads will remain bound to wait for new requests to arrive.")
    public static final Setting<Duration> thread_binding_timeout = newBuilder(
                    "internal.dbms.bolt.thread_binding_timeout", DURATION, Duration.ofMillis(10))
            .addConstraint(min(Duration.ZERO))
            .build();

    @Internal
    @Description("Specifies the string used to connect to the local channel")
    public static final Setting<String> local_channel_address =
            newBuilder("internal.dbms.bolt.local_address", STRING, null).build();

    @Internal
    @Description("Enabled of disable local bolt connector.")
    public static final Setting<Boolean> enable_local_connector =
            newBuilder("internal.dbms.bolt.local_enabled", BOOL, true).build();

    @Internal
    @Description("Enabled of disabled object messages on local bolt connector .")
    public static final Setting<Boolean> enable_object_messages_local_connector =
            newBuilder("internal.dbms.bolt.local_object_enabled", BOOL, false).build();

    @Internal
    @Description("Define protocol version of object messages on local bolt connector .")
    public static final Setting<ConfiguredProtocolVersion> enable_object_messages_protocol_version_local_connector =
            newBuilder(
                            "internal.dbms.bolt.local_object_protocol_version",
                            PROTOCOL_VERSION,
                            ConfiguredProtocolVersion.fromString("6.1"))
                    .build();

    @Internal
    @Description(
            "Permits the use of user-databases (databases other than \"system\") via the Unix Domain Socket connector.")
    public static final Setting<Boolean> enable_unix_socket_user_database_access = newBuilder(
                    "internal.dbms.bolt.unix_socket_permit_unix_socket_user_database_access", BOOL, false)
            .build();

    @Internal
    @Description("Minimum Bolt Protocol version negotiated by the bolt connector.")
    public static final Setting<ConfiguredProtocolVersion> min_protocol_version = newBuilder(
                    "internal.dbms.bolt.min_protocol_version", PROTOCOL_VERSION, null)
            .build();

    @Internal
    @Description("Maximum Bolt Protocol version negotiated by the bolt connector.")
    public static final Setting<ConfiguredProtocolVersion> max_protocol_version = newBuilder(
                    "internal.dbms.bolt.max_protocol_version", PROTOCOL_VERSION, null)
            .build();

    @Internal
    @Description("A list of masks permitted for use with the fleet discovery protocol.")
    public static final Setting<List<IPAddressString>> discovery_network_masks = newBuilder(
                    "internal.dbms.fleet_discovery.permitted_network_masks",
                    listOf(CIDR_IP),
                    List.of(
                            new IPAddressString("10.0.0.0/8"),
                            new IPAddressString("169.254.0.0/16"),
                            new IPAddressString("172.16.0.0/12"),
                            new IPAddressString("192.168.0.0/16")))
            .addConstraint(minSize(1))
            .build();

    @Internal
    @Description("Period of time between thread liveliness checks (zero disables checks)")
    public static final Setting<Duration> thread_accountant_check_period = newBuilder(
                    "internal.dbms.bolt.thread_accountant_check_period", DURATION, Duration.ofSeconds(10))
            .addConstraint(min(Duration.ZERO))
            .build();

    @Internal
    @Description("Maximum duration for which a thread may be occupied before reporting it")
    public static final Setting<Duration> thread_accountant_max_run_time = newBuilder(
                    "internal.dbms.bolt.thread_accountant_max_run_time", DURATION, Duration.ofMinutes(10))
            .addConstraint(min(Duration.ofSeconds(30)))
            .build();

    public enum ProtocolLoggingMode {
        DECODED(false, true),
        RAW(true, false),
        BOTH(true, true);

        private final boolean loggingRawTraffic;
        private final boolean loggingDecodedTraffic;

        ProtocolLoggingMode(boolean loggingRawTraffic, boolean loggingDecodedTraffic) {
            this.loggingRawTraffic = loggingRawTraffic;
            this.loggingDecodedTraffic = loggingDecodedTraffic;
        }

        public boolean isLoggingRawTraffic() {
            return loggingRawTraffic;
        }

        public boolean isLoggingDecodedTraffic() {
            return loggingDecodedTraffic;
        }
    }

    public record ConfiguredProtocolVersion(Integer major, Integer minor) {
        @Override
        public String toString() {
            return major + "." + minor;
        }

        public static ConfiguredProtocolVersion fromString(String value) {
            String trimmedValue = value.trim();

            return Optional.of(trimmedValue)
                    .map(trimmed -> trimmed.split("\\."))
                    .filter(partsArr -> partsArr.length == 2)
                    .map(List::of)
                    .map(partsList -> partsList.stream()
                            .map(maybeInt -> {
                                try {
                                    return Integer.parseInt(maybeInt);
                                } catch (NumberFormatException ignored) {
                                    return null;
                                }
                            })
                            .filter(Objects::nonNull)
                            .toList())
                    .filter(partsInt -> partsInt.size() == 2)
                    .map(partsInt -> new ConfiguredProtocolVersion(partsInt.get(0), partsInt.get(1)))
                    .orElseThrow(() -> new IllegalArgumentException(
                            format("'%s' is not a valid protocol version value, must be '<major>.<minor>'", value)));
        }
    }
}
