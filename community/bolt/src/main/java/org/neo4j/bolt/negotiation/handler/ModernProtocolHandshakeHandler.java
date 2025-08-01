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
package org.neo4j.bolt.negotiation.handler;

import io.netty.channel.ChannelHandlerContext;
import java.util.EnumSet;
import java.util.stream.Collectors;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.codec.ModernProtocolNegotiationFinalizeMessageDecoder;
import org.neo4j.bolt.negotiation.codec.ModernProtocolNegotiationInitMessageEncoder;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationFinalizeMessage;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationInitMessage;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;

public final class ModernProtocolHandshakeHandler
        extends AbstractProtocolHandshakeHandler<ModernProtocolNegotiationFinalizeMessage> {

    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ModernProtocolHandshakeHandler.class);

    private BoltProtocolRegistry protocolRegistry;

    public ModernProtocolHandshakeHandler(InternalLogProvider logging) {
        super(logging);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);

        this.protocolRegistry = this.connector.protocolRegistry();

        var versions = protocolRegistry.versionsAvailable();

        ctx.writeAndFlush(new ModernProtocolNegotiationInitMessage(
                ProtocolVersion.NEGOTIATION_V2, versions, this.connector.supportedCapabilities()));
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        this.connection.memoryTracker().releaseHeap(SHALLOW_SIZE);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ModernProtocolNegotiationFinalizeMessage msg)
            throws Exception {
        if (ProtocolVersion.INVALID.equals(msg.selectedVersion())) {
            log.debug(
                    "Failed Bolt handshake: Client does not support any of the proposed Bolt versions supported by this server.");
            ctx.close();
            return;
        }

        var selectedProtocol = this.protocolRegistry.get(msg.selectedVersion()).orElse(null);

        if (selectedProtocol == null) {
            log.debug(
                    "Failed Bolt handshake: Bolt version suggested by client '%s' are not supported by this server.",
                    msg.selectedVersion());

            ctx.close();
            return;
        }

        // ensure that the client has only provided capabilities that were initially indicated to be
        // supported by the connector
        var connectorCapabilities = this.connector.supportedCapabilities();
        if (msg.capabilities().stream().anyMatch(capability -> !connectorCapabilities.contains(capability))) {
            var mismatch = msg.capabilities().stream()
                    .filter(capability -> !connectorCapabilities.contains(capability))
                    .map(Enum::name)
                    .collect(Collectors.joining(", "));

            log.debug(
                    "Failed Bolt handshake: One or more capabilities suggested by client '%s' are not supported by this connector.",
                    mismatch);

            ctx.close();
            return;
        }

        var capabilities = EnumSet.copyOf(msg.capabilities());
        capabilities.add(ProtocolCapability.HANDSHAKE_V2);

        this.finalizeHandshake(ctx, selectedProtocol, capabilities);
    }

    @Override
    protected void removeStageHandlers(ChannelHandlerContext ctx) {
        super.removeStageHandlers(ctx);

        ctx.pipeline().remove(ModernProtocolNegotiationInitMessageEncoder.class);
        ctx.pipeline().remove(ModernProtocolNegotiationFinalizeMessageDecoder.class);
    }
}
