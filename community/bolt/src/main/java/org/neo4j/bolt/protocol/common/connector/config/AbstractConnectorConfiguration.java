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
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ProtocolLoggingMode;

public abstract class AbstractConnectorConfiguration implements ConnectorConfiguration {

    private final boolean enableProtocolCapture;
    private final Path protocolCapturePath;
    private final boolean enableProtocolLogging;
    private final ProtocolLoggingMode protocolLoggingMode;
    private final long maxAuthenticationInboundBytes;
    private final int maxAuthenticationStructureElements;
    private final int maxAuthenticationStructureDepth;
    private final boolean enableOutboundBufferThrottle;
    private final int outboundBufferThrottleLowWatermark;
    private final int outboundBufferThrottleHighWatermark;
    private final Duration outboundBufferMaxThrottleDuration;
    private final int inboundBufferThrottleLowWatermark;
    private final int inboundBufferThrottleHighWatermark;
    private final int streamingBufferSize;
    private final int streamingFlushThreshold;
    private final Duration connectionShutdownDuration;
    private final boolean enableTransactionThreadBinding;
    private final Duration threadBindingTimeout;
    private final boolean isInternalConnector;
    private final boolean enableJavaObjectMessages;

    protected AbstractConnectorConfiguration(AbstractFactory<?> builder) {
        this.enableProtocolCapture = builder.enableProtocolCapture;
        this.protocolCapturePath = builder.protocolCapturePath;
        this.enableProtocolLogging = builder.enableProtocolLogging;
        this.protocolLoggingMode = builder.protocolLoggingMode;
        this.maxAuthenticationInboundBytes = builder.maxAuthenticationInboundBytes;
        this.maxAuthenticationStructureElements = builder.maxAuthenticationStructureElements;
        this.maxAuthenticationStructureDepth = builder.maxAuthenticationStructureDepth;
        this.enableOutboundBufferThrottle = builder.enableOutboundBufferThrottle;
        this.outboundBufferThrottleLowWatermark = builder.outboundBufferThrottleLowWatermark;
        this.outboundBufferThrottleHighWatermark = builder.outboundBufferThrottleHighWatermark;
        this.outboundBufferMaxThrottleDuration = builder.outboundBufferMaxThrottleDuration;
        this.inboundBufferThrottleLowWatermark = builder.inboundBufferThrottleLowWatermark;
        this.inboundBufferThrottleHighWatermark = builder.inboundBufferThrottleHighWatermark;
        this.streamingBufferSize = builder.streamingBufferSize;
        this.streamingFlushThreshold = builder.streamingFlushThreshold;
        this.connectionShutdownDuration = builder.connectionShutdownDuration;
        this.enableTransactionThreadBinding = builder.enableTransactionThreadBinding;
        this.threadBindingTimeout = builder.threadBindingTimeout;
        this.isInternalConnector = builder.isInternalConnector;
        this.enableJavaObjectMessages = builder.enableJavaObjectMessages;
    }

    @Override
    public boolean enableProtocolCapture() {
        return this.enableProtocolCapture;
    }

    @Override
    public Path protocolCapturePath() {
        return this.protocolCapturePath;
    }

    @Override
    public boolean enableProtocolLogging() {
        return this.enableProtocolLogging;
    }

    @Override
    public ProtocolLoggingMode protocolLoggingMode() {
        return this.protocolLoggingMode;
    }

    @Override
    public long maxAuthenticationInboundBytes() {
        return this.maxAuthenticationInboundBytes;
    }

    @Override
    public int maxAuthenticationStructureElements() {
        return this.maxAuthenticationStructureElements;
    }

    @Override
    public int maxAuthenticationStructureDepth() {
        return this.maxAuthenticationStructureDepth;
    }

    @Override
    public boolean enableOutboundBufferThrottle() {
        return this.enableOutboundBufferThrottle;
    }

    @Override
    public int outboundBufferThrottleLowWatermark() {
        return this.outboundBufferThrottleLowWatermark;
    }

    @Override
    public int outboundBufferThrottleHighWatermark() {
        return this.outboundBufferThrottleHighWatermark;
    }

    @Override
    public Duration outboundBufferMaxThrottleDuration() {
        return this.outboundBufferMaxThrottleDuration;
    }

    @Override
    public int inboundBufferThrottleLowWatermark() {
        return this.inboundBufferThrottleLowWatermark;
    }

    @Override
    public int inboundBufferThrottleHighWatermark() {
        return this.inboundBufferThrottleHighWatermark;
    }

    @Override
    public int streamingBufferSize() {
        return this.streamingBufferSize;
    }

    @Override
    public int streamingFlushThreshold() {
        return this.streamingFlushThreshold;
    }

    @Override
    public Duration connectionShutdownDuration() {
        return this.connectionShutdownDuration;
    }

    @Override
    public boolean enableTransactionThreadBinding() {
        return this.enableTransactionThreadBinding;
    }

    @Override
    public boolean isInternalConnector() {
        return this.isInternalConnector;
    }

    @Override
    public Duration threadBindingTimeout() {
        return this.threadBindingTimeout;
    }

    @Override
    public boolean enableJavaObjectMessages() {
        return this.enableJavaObjectMessages;
    }

    @SuppressWarnings("unchecked")
    public abstract static class AbstractFactory<SELF extends AbstractFactory<SELF>>
            implements ConnectorConfiguration.Factory<SELF> {

        private boolean enableProtocolCapture = false;
        private Path protocolCapturePath = null;
        private boolean enableProtocolLogging = false;
        private ProtocolLoggingMode protocolLoggingMode = ProtocolLoggingMode.DECODED;
        private long maxAuthenticationInboundBytes = Long.MAX_VALUE;
        private int maxAuthenticationStructureElements = Integer.MAX_VALUE;
        private int maxAuthenticationStructureDepth = Integer.MAX_VALUE;
        private boolean enableOutboundBufferThrottle = false;
        private int outboundBufferThrottleLowWatermark = Integer.MAX_VALUE;
        private int outboundBufferThrottleHighWatermark = Integer.MAX_VALUE;
        private Duration outboundBufferMaxThrottleDuration = Duration.ofMinutes(5);
        private int inboundBufferThrottleLowWatermark = Integer.MAX_VALUE;
        private int inboundBufferThrottleHighWatermark = Integer.MAX_VALUE;
        private int streamingBufferSize = 256;
        private int streamingFlushThreshold = 8192;
        private Duration connectionShutdownDuration = Duration.ofMinutes(5);
        private boolean enableTransactionThreadBinding = true;
        private Duration threadBindingTimeout = Duration.ofMillis(100);
        private boolean isInternalConnector = false;
        private boolean enableJavaObjectMessages = false;

        @Override
        public SELF fromConfig(Config config) {
            this.enableProtocolCapture = config.get(BoltConnectorInternalSettings.protocol_capture);
            this.protocolCapturePath = config.get(BoltConnectorInternalSettings.protocol_capture_path);

            this.enableProtocolLogging = config.get(BoltConnectorInternalSettings.protocol_logging);
            this.protocolLoggingMode = config.get(BoltConnectorInternalSettings.protocol_logging_mode);

            this.maxAuthenticationInboundBytes =
                    config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes);
            this.maxAuthenticationStructureElements =
                    config.get(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_elements);
            this.maxAuthenticationStructureDepth =
                    config.get(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_depth);

            this.enableOutboundBufferThrottle = config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle);
            this.outboundBufferThrottleLowWatermark =
                    config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_low_water_mark);
            this.outboundBufferThrottleHighWatermark =
                    config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_high_water_mark);
            this.outboundBufferMaxThrottleDuration =
                    config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_max_duration);
            this.inboundBufferThrottleLowWatermark =
                    config.get(BoltConnectorInternalSettings.bolt_inbound_message_throttle_low_water_mark);
            this.inboundBufferThrottleHighWatermark =
                    config.get(BoltConnectorInternalSettings.bolt_inbound_message_throttle_high_water_mark);

            this.streamingBufferSize = config.get(BoltConnectorInternalSettings.streaming_buffer_size);
            this.streamingFlushThreshold = config.get(BoltConnectorInternalSettings.streaming_flush_threshold);

            this.enableTransactionThreadBinding = config.get(BoltConnectorInternalSettings.transaction_thread_binding);
            this.threadBindingTimeout = config.get(BoltConnectorInternalSettings.thread_binding_timeout);
            this.connectionShutdownDuration = config.get(BoltConnectorInternalSettings.connection_shutdown_wait_time);

            return (SELF) this;
        }

        @Override
        public SELF enableProtocolCapture(boolean value) {
            this.enableProtocolCapture = value;
            return (SELF) this;
        }

        @Override
        public SELF protocolCapturePath(Path value) {
            this.protocolCapturePath = value;
            return (SELF) this;
        }

        @Override
        public SELF enableProtocolLogging(boolean value) {
            this.enableProtocolLogging = value;
            return (SELF) this;
        }

        @Override
        public SELF protocolLoggingMode(ProtocolLoggingMode value) {
            this.protocolLoggingMode = value;
            return (SELF) this;
        }

        @Override
        public SELF maxAuthenticationInboundBytes(long value) {
            this.maxAuthenticationInboundBytes = value;
            return (SELF) this;
        }

        @Override
        public SELF maxAuthenticationStructureElements(int value) {
            this.maxAuthenticationStructureElements = value;
            return (SELF) this;
        }

        @Override
        public SELF maxAuthenticationStructureDepth(int value) {
            this.maxAuthenticationStructureDepth = value;
            return (SELF) this;
        }

        @Override
        public SELF enableOutboundBufferThrottle(boolean value) {
            this.enableOutboundBufferThrottle = value;
            return (SELF) this;
        }

        @Override
        public SELF outboundBufferThrottleLowWatermark(int value) {
            this.outboundBufferThrottleLowWatermark = value;
            return (SELF) this;
        }

        @Override
        public SELF outboundBufferThrottleHighWatermark(int value) {
            this.outboundBufferThrottleHighWatermark = value;
            return (SELF) this;
        }

        @Override
        public SELF outboundBufferMaxThrottleDuration(Duration value) {
            this.outboundBufferMaxThrottleDuration = value;
            return (SELF) this;
        }

        @Override
        public SELF inboundBufferThrottleLowWatermark(int value) {
            this.inboundBufferThrottleLowWatermark = value;
            return (SELF) this;
        }

        @Override
        public SELF inboundBufferThrottleHighWatermark(int value) {
            this.inboundBufferThrottleHighWatermark = value;
            return (SELF) this;
        }

        @Override
        public SELF streamingBufferSize(int value) {
            this.streamingBufferSize = value;
            return (SELF) this;
        }

        @Override
        public SELF streamingFlushThreshold(int value) {
            this.streamingFlushThreshold = value;
            return (SELF) this;
        }

        @Override
        public SELF connectionShutdownDuration(Duration value) {
            this.connectionShutdownDuration = value;
            return (SELF) this;
        }

        @Override
        public SELF enableTransactionThreadBinding(boolean value) {
            this.enableTransactionThreadBinding = value;
            return (SELF) this;
        }

        @Override
        public SELF threadBindingTimeout(Duration value) {
            this.threadBindingTimeout = value;
            return (SELF) this;
        }

        @Override
        public SELF isInternalConnector(boolean value) {
            this.isInternalConnector = value;
            return (SELF) this;
        }

        @Override
        public SELF enableJavaObjectMessages(boolean value) {
            this.enableJavaObjectMessages = value;
            return (SELF) this;
        }
    }
}
