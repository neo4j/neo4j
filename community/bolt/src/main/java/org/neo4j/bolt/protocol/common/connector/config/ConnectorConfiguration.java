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
package org.neo4j.bolt.protocol.common.connector.config;

import java.nio.file.Path;
import java.time.Duration;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ProtocolLoggingMode;

public interface ConnectorConfiguration {

    /**
     * Identifies whether protocol capture has been enabled for this connector.
     * <p/>
     * When true, PCAP encoded versions of the protocol traffic passing through this connector will be
     * written to disk for debugging purposes.
     *
     * @return true if protocol capture is enabled, false otherwise.
     */
    boolean enableProtocolCapture();

    /**
     * Identifies the directory to which protocol captures should be written when enabled.
     * <p/>
     * This configuration property is ignored when {@link #enableProtocolCapture()} is set to false.
     *
     * @return a protocol capture directory.
     */
    Path protocolCapturePath();

    /**
     * Identifies whether protocol logging has been enabled for this connector.
     * <p/>
     * When enabled, all protocol traffic within this connector will be logged to the application
     * debug log.
     *
     * @return true if protocol logging is enabled, false otherwise.
     */
    boolean enableProtocolLogging();

    /**
     * Identifies the detail to be logged via this connector.
     * <p/>
     * This configuration property is ignored if {@link #enableProtocolLogging()} is set to false.
     *
     * @return a protocol logging mode.
     */
    ProtocolLoggingMode protocolLoggingMode();

    /**
     * Identifies the total amount of bytes permitted to be sent to the server during authentication
     * phase.
     * <p/>
     * When a client exceeds this amount of data during the authentication phase, their connection
     * will be terminated prematurely.
     *
     * @return a maximum amount of permitted bytes.
     */
    long maxAuthenticationInboundBytes();

    /**
     * Identifies the total amount of elements permitted to be present within a structure during
     * authentication phase.
     * <p/>
     * When a client exceeds this amount of elements during the authentication phase, their connection
     * will be terminated prematurely.
     *
     * @return a maximum amount of Packstream elements.
     */
    int maxAuthenticationStructureElements();

    /**
     * Identifies the total amount of nested levels permitted to be present within a structure during
     * authentication phase.
     * <p/>
     * When a client exceeds this amount of elements during the authentication phase, their connection
     * will be terminated prematurely.
     *
     * @return a maximum amount of Packstream nesting levels.
     */
    int maxAuthenticationStructureDepth();

    /**
     * Identifies whether outbound messages shall be throttled when the client fails to consume
     * messages at a sufficient pace.
     *
     * @return true if outbound buffer throttling shall be applied.
     */
    boolean enableOutboundBufferThrottle();

    /**
     * Identifies the minimum amount of bytes present within the outgoing buffer prior to releasing
     * previously triggered outbound throttling measures.
     *
     * @return an outbound throttle low watermark.
     */
    int outboundBufferThrottleLowWatermark();

    /**
     * Identifies the maximum amount of bytes present within the outgoing buffer prior to triggering
     * outbound throttling measures.
     * <p/>
     * When {@link #enableOutboundBufferThrottle()} is enabled and more than the specified amount of
     * bytes accumulates within the outgoing buffer, writing of data will be suspended until the
     * client has consumed enough data to return the buffer to below
     * {@link #outboundBufferThrottleHighWatermark()} bytes.
     *
     * @return an outbound throttle high watermark.
     */
    int outboundBufferThrottleHighWatermark();

    /**
     * Identifies the maximum amount of time a connection is permitted to remain within an outbound
     * throttled state prior to being terminated.
     * <p/>
     * This setting is provided in order to prevent slow or potentially locked up drivers from
     * indefinitely claiming server resources.
     *
     * @return a maximum outbound throttle duration.
     */
    Duration outboundBufferMaxThrottleDuration();

    /**
     * Identifies the minimum amount of bytes present within the inbound buffer before releasing
     * previously triggered outbound throttling measures.
     *
     * @return an inbound throttle low watermark.
     */
    int inboundBufferThrottleLowWatermark();

    /**
     * Identifies the maximum amount of bytes present within the inbound buffer prior to disabling the
     * decoder pipeline.
     * <p/>
     * Once reached, the server will disable the decoder pipeline for the offending connection until
     * {@link #inboundBufferThrottleLowWatermark()} is reached as a result of slowly processing
     * already buffered messages.
     *
     * @return an inbound throttle high watermark.
     */
    int inboundBufferThrottleHighWatermark();

    /**
     * Identifies the total number of bytes to be requested from the pool when streaming records.
     * <p/>
     * Lower values may improve overall operation performance but may result in more frequent
     * allocations when larger values are streamed at regular intervals.
     *
     * @return a buffer size (in bytes).
     */
    int streamingBufferSize();

    /**
     * Identifies the total number of bytes expected to be present within the outgoing record buffer
     * while streaming before the buffers are flushed.
     * <p/>
     * Lower values will improve overall latency but might impact performance as records are more
     * frequently flushed to the client.
     * <p/>
     * Flushing always occurs upon completion of a streaming operation regardless of the value
     * configured within this property.
     * <p/>
     * A value of zero indicates no threshold (e.g. flushing occurs on a per-record basis).
     *
     * @return a flush threshold (in bytes).
     */
    int streamingFlushThreshold();

    /**
     * Identifies the duration for which the connector shall wait when terminating connections before
     * considering its workload to be stuck.
     *
     * @return a maximum permissible shutdown duration.
     */
    Duration connectionShutdownDuration();

    /**
     * Identifies whether transactions shall be bound to threads for the duration of their lifetime.
     *
     * @return true if transactions shall occupy threads for their lifetime, false otherwise.
     */
    boolean enableTransactionThreadBinding();

    /**
     * Identifies whether this connector is an internal connector.
     * <p/>
     * This allows the server to distinguish between internal and external connectors, if features are
     * not applicable to one or the other.
     *
     * @return true if this is an internal connector, false otherwise.
     */
    boolean isInternalConnector();

    /**
     * Specifies the total duration for which a thread is bound to a given connection when no requests
     * remain to be processed.
     * <p/>
     * This value is provided as an optimization in order to reduce rapid re-scheduling of connections
     * within short windows of inactivity (e.g. due to network latency).
     * <p/>
     * When set to zero, threads will be freed up immediately once no more requests remain to be
     * processed.
     *
     * @return a timeout duration after which a thread is released.
     */
    Duration threadBindingTimeout();

    /**
     * Enables Bolt Messages being shared without being encoded in Packstream.
     * <p>
     * When enabled, the protocol version negotiation will be skipped. The lastest
     * Bolt version will be used.
     *
     * @return the connection will talk plain bolt messages
     */
    boolean enableJavaObjectMessages();

    interface Factory<SELF extends Factory<SELF>> {

        ConnectorConfiguration build();

        SELF fromConfig(Config config);

        SELF enableProtocolCapture(boolean value);

        SELF protocolCapturePath(Path value);

        default SELF enableProtocolLogging(ProtocolLoggingMode mode) {
            return this.enableProtocolLogging(true).protocolLoggingMode(mode);
        }

        SELF enableProtocolLogging(boolean value);

        SELF protocolLoggingMode(ProtocolLoggingMode value);

        SELF maxAuthenticationInboundBytes(long value);

        SELF maxAuthenticationStructureElements(int value);

        SELF maxAuthenticationStructureDepth(int value);

        SELF enableOutboundBufferThrottle(boolean value);

        SELF outboundBufferThrottleLowWatermark(int value);

        SELF outboundBufferThrottleHighWatermark(int value);

        SELF outboundBufferMaxThrottleDuration(Duration value);

        default SELF enableInboundBufferThrottle(int lowWatermark, int highWatermark) {
            return this.inboundBufferThrottleLowWatermark(lowWatermark)
                    .inboundBufferThrottleHighWatermark(highWatermark);
        }

        SELF inboundBufferThrottleLowWatermark(int value);

        SELF inboundBufferThrottleHighWatermark(int value);

        SELF streamingBufferSize(int value);

        SELF streamingFlushThreshold(int value);

        SELF connectionShutdownDuration(Duration value);

        SELF enableTransactionThreadBinding(boolean value);

        SELF threadBindingTimeout(Duration value);

        SELF isInternalConnector(boolean value);

        SELF enableJavaObjectMessages(boolean embeddedMessages);
    }
}
