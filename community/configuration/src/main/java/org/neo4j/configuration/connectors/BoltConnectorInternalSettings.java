/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.configuration.connectors;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.SettingConstraints.any;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.nio.file.Path;
import java.time.Duration;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;

@ServiceProvider
public final class BoltConnectorInternalSettings implements SettingsDeclaration {

    public static final String LOOPBACK_NAME = "bolt-loopback";

    @Internal
    @Description("Enable protocol level logging for incoming connections on the Bolt connector")
    public static final Setting<Boolean> protocol_logging =
            newBuilder("internal.server.bolt.protocol_logging", BOOL, false).build();

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
    @Description("The absolute path of the file for use with the Unix Domain Socket based loopback interface. "
            + "This file must be specified and will be created at runtime and deleted on shutdown.")
    public static final Setting<Path> unsupported_loopback_listen_file =
            newBuilder("internal.dbms.loopback_file", PATH, null).build();

    @Internal
    @Description(
            "Whether or not to delete an existing file for use with the Unix Domain Socket based loopback interface. "
                    + "This improves the handling of the case where a previous hard shutdown was unable to delete the file.")
    public static final Setting<Boolean> unsupported_loopback_delete =
            newBuilder("internal.dbms.loopback_delete", BOOL, false).build();

    @Internal
    @Description("Enable or disable the bolt loopback connector. "
            + "A user successfully authenticated over this will execute all queries with no security restrictions. "
            + "This includes overriding the `"
            + "internal.dbms.block_create_drop_database" + "`, " + "`"
            + "internal.dbms.block_start_stop_database" + "` and `" + "internal.dbms.upgrade_restriction_enabled"
            + "` settings.")
    public static final Setting<Boolean> enable_loopback_auth =
            newBuilder("internal.dbms.loopback_enabled", BOOL, false).build();

    @Internal
    @Description("The maximum time to wait for the thread pool to finish processing its pending jobs and shutdown")
    public static final Setting<Duration> thread_pool_shutdown_wait_time = newBuilder(
                    "internal.server.bolt.thread_pool_shutdown_wait_time", DURATION, ofSeconds(5))
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
    @Description("Enable/disable generation of response metrics")
    public static final Setting<Boolean> enable_response_metrics =
            newBuilder("internal.server.bolt.response_metrics", BOOL, false).build();
}
