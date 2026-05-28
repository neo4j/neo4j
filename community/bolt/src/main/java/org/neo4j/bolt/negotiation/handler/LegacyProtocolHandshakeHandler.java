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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.util.Arrays;
import java.util.EnumSet;
import org.neo4j.bolt.negotiation.codec.ModernProtocolNegotiationFinalizeMessageDecoder;
import org.neo4j.bolt.negotiation.codec.ModernProtocolNegotiationInitMessageEncoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationRequestDecoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationResponseEncoder;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationRequest;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationResponse;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;

public final class LegacyProtocolHandshakeHandler extends AbstractProtocolHandshakeHandler<ProtocolNegotiationRequest> {

    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(LegacyProtocolHandshakeHandler.class);

    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;

    public LegacyProtocolHandshakeHandler(InternalLogProvider logging) {
        super(logging);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        this.connection.memoryTracker().releaseHeap(SHALLOW_SIZE);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ProtocolNegotiationRequest request) throws Exception {
        // ensure we've received the correct magic number - otherwise just close the connection immediately as we
        // cannot verify that we are talking to a bolt compatible client
        if (request.getMagicNumber() != BOLT_MAGIC_PREAMBLE) {
            log.debug(
                    "Invalid Bolt handshake signature. Expected 0x%08X, but got: 0x%08X",
                    BOLT_MAGIC_PREAMBLE, request.getMagicNumber());
            ctx.close();
            return;
        }

        // go through every suggested protocol revision (in order of occurrence) and check whether we are able to
        // satisfy it (if so - move on)
        BoltProtocol selectedProtocol = null;
        var protocolRegistry = this.connector.protocolRegistry();
        var it = request.proposedVersions().iterator();
        do {
            // if the list has been exhausted, then none of the suggested protocol versions is supported by the
            // server - notify client and abort
            if (!it.hasNext()) {
                log.debug(
                        "Failed Bolt handshake: Bolt versions suggested by client '%s' are not supported by this server.",
                        Arrays.toString(request.proposedVersions().toArray()));

                ctx.writeAndFlush(new ProtocolNegotiationResponse(ProtocolVersion.INVALID))
                        .addListener(ChannelFutureListener.CLOSE);

                return;
            }

            var proposal = it.next();

            // invalid protocol versions are passed to pad the request when less than four unique version ranges are
            // supported by the client - ignore them
            if (ProtocolVersion.INVALID.equals(proposal)) {
                continue;
            }

            // check whether the proposal is for a modern handshake where the response scheme is
            // inverted
            if (proposal.isNegotiationVersion()) {
                // currently we only support a single revision of the handshake, so if the client
                // can't understand us, we'll fail here
                if (!proposal.matches(ProtocolVersion.NEGOTIATION_V2)) {
                    log.debug(
                            "Rejected modern handshake: Handshake version(s) suggested by client '%s' are not supported by this server.",
                            proposal);

                    // we'll give the client a chance to negotiate a different version further down
                    // the line
                    continue;
                }

                this.handleModernHandshake(ctx);

                // abort selection process here as the protocol will switch in entirety given that
                // we understood this request
                return;
            }

            selectedProtocol = protocolRegistry.get(proposal).orElse(null);
        } while (selectedProtocol == null);

        // finalize the handshake using legacy messages - no capabilities are supported in this
        // version of the handshake, so the set will always be empty
        this.finalizeHandshake(ctx, selectedProtocol, EnumSet.noneOf(ProtocolCapability.class));
    }

    private void handleModernHandshake(ChannelHandlerContext ctx) {
        this.connection
                .memoryTracker()
                .allocateHeap(ModernProtocolHandshakeHandler.SHALLOW_SIZE
                        + ModernProtocolNegotiationInitMessageEncoder.SHALLOW_SIZE
                        + ModernProtocolNegotiationFinalizeMessageDecoder.SHALLOW_SIZE);

        ctx.pipeline()
                .addLast(new ModernProtocolNegotiationInitMessageEncoder())
                .addLast(new ModernProtocolNegotiationFinalizeMessageDecoder())
                .addLast(new ModernProtocolHandshakeHandler(this.logging));

        this.removeStageHandlers(ctx);
    }

    @Override
    protected void onVersionSelected(ChannelHandlerContext ctx, BoltProtocol protocol) {
        ctx.writeAndFlush(new ProtocolNegotiationResponse(protocol.version()));
    }

    @Override
    protected void removeStageHandlers(ChannelHandlerContext ctx) {
        super.removeStageHandlers(ctx);

        ctx.pipeline().remove(ProtocolNegotiationResponseEncoder.class);
        ctx.pipeline().remove(ProtocolNegotiationRequestDecoder.class);
    }
}
