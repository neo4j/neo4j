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
package org.neo4j.bolt.testing.mock;

import io.netty.handler.ssl.SslContext;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Consumer;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector.NettyConfiguration;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ProtocolLoggingMode;

public final class ConnectorConfigurationMockFactory
        extends AbstractMockFactory<NettyConfiguration, ConnectorConfigurationMockFactory> {

    private ConnectorConfigurationMockFactory() {
        super(NettyConfiguration.class);
    }

    public static ConnectorConfigurationMockFactory newFactory() {
        return new ConnectorConfigurationMockFactory();
    }

    public static NettyConfiguration newInstance() {
        return newFactory().build();
    }

    public static NettyConfiguration newInstance(Consumer<ConnectorConfigurationMockFactory> configurer) {
        var factory = newFactory();
        configurer.accept(factory);
        return factory.build();
    }

    public ConnectorConfigurationMockFactory withoutProtocolCapture() {
        return this.withStaticValue(NettyConfiguration::enableProtocolCapture, false);
    }

    public ConnectorConfigurationMockFactory withProtocolCapture(Path path) {
        return this.withStaticValue(NettyConfiguration::enableProtocolCapture, true)
                .withStaticValue(NettyConfiguration::protocolCapturePath, path);
    }

    public ConnectorConfigurationMockFactory withoutProtocolLogging() {
        return this.withStaticValue(NettyConfiguration::enableProtocolLogging, false);
    }

    public ConnectorConfigurationMockFactory withProtocolLogging(ProtocolLoggingMode mode) {
        return this.withStaticValue(NettyConfiguration::enableProtocolLogging, true)
                .withStaticValue(NettyConfiguration::protocolLoggingMode, mode);
    }

    public ConnectorConfigurationMockFactory withRequiresEncryption(boolean value) {
        return this.withStaticValue(NettyConfiguration::requiresEncryption, value);
    }

    public ConnectorConfigurationMockFactory withEnableMergeCumulator(boolean value) {
        return this.withStaticValue(NettyConfiguration::enableMergeCumulator, value);
    }

    public ConnectorConfigurationMockFactory withSslContext(SslContext value) {
        return this.withStaticValue(NettyConfiguration::sslContext, value);
    }

    public ConnectorConfigurationMockFactory withMaxAuthenticationInboundBytes(long value) {
        return this.withStaticValue(NettyConfiguration::maxAuthenticationInboundBytes, value);
    }

    public ConnectorConfigurationMockFactory withoutOutboundBufferThrottle() {
        return this.withStaticValue(NettyConfiguration::enableOutboundBufferThrottle, false);
    }

    public ConnectorConfigurationMockFactory withOutboundBufferThrottle(
            int lowWatermark, int highWatermark, Duration maxDuration) {
        return this.withStaticValue(NettyConfiguration::enableOutboundBufferThrottle, true)
                .withStaticValue(NettyConfiguration::outboundBufferThrottleLowWatermark, lowWatermark)
                .withStaticValue(NettyConfiguration::outboundBufferThrottleHighWatermark, highWatermark)
                .withStaticValue(NettyConfiguration::outboundBufferMaxThrottleDuration, maxDuration);
    }

    public ConnectorConfigurationMockFactory withInboundBufferThrottle(int lowWatermark, int highWatermark) {
        return this.withStaticValue(NettyConfiguration::inboundBufferThrottleLowWatermark, lowWatermark)
                .withStaticValue(NettyConfiguration::inboundBufferThrottleHighWatermark, highWatermark);
    }

    public ConnectorConfigurationMockFactory withStreamingBufferSize(int value) {
        return this.withStaticValue(NettyConfiguration::streamingBufferSize, value);
    }

    public ConnectorConfigurationMockFactory withStreamingFlushThreshold(int value) {
        return this.withStaticValue(NettyConfiguration::streamingFlushThreshold, value);
    }

    public ConnectorConfigurationMockFactory withConnectionShutdownDuration(Duration value) {
        return this.withStaticValue(NettyConfiguration::connectionShutdownDuration, value);
    }
}
