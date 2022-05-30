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
package org.neo4j.bolt.protocol.common.handler;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import java.util.Arrays;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationRequestDecoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationResponseEncoder;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationRequest;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationResponse;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.codec.BoltStructEncoder;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionFactory;
import org.neo4j.bolt.protocol.common.handler.messages.GoodbyeMessageHandler;
import org.neo4j.bolt.protocol.common.handler.messages.ResetMessageHandler;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.protocol.common.transaction.result.ResultHandler;
import org.neo4j.bolt.runtime.throttle.ChannelThrottleHandler;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.codec.PackstreamStructDecoder;
import org.neo4j.packstream.codec.PackstreamStructEncoder;
import org.neo4j.packstream.codec.transport.ChunkFrameDecoder;
import org.neo4j.packstream.codec.transport.ChunkFrameEncoder;
import org.neo4j.packstream.codec.transport.FrameSignalEncoder;

public class ProtocolHandshakeHandler extends SimpleChannelInboundHandler<ProtocolNegotiationRequest> {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ProtocolHandshakeHandler.class);

    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;

    private final BoltChannel channel;
    private final BoltProtocolRegistry protocolRegistry;
    private final BoltConnectionFactory connectionFactory;

    private final InternalLogProvider logging;
    private final InternalLog log;
    private final Config config;

    public ProtocolHandshakeHandler(
            BoltProtocolRegistry protocolRegistry,
            BoltConnectionFactory connectionFactory,
            BoltChannel channel,
            InternalLogProvider logging,
            Config config) {
        this.protocolRegistry = protocolRegistry;
        this.connectionFactory = connectionFactory;
        this.channel = channel;
        this.logging = logging;
        this.log = logging.getLog(getClass());
        this.config = config;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        channel.memoryTracker().releaseHeap(SHALLOW_SIZE);
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
        BoltProtocol protocol = null;
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

            protocol = protocolRegistry.get(proposal, channel).orElse(null);
        } while (protocol == null);

        var stateMachine = protocol.createStateMachine(channel);
        var connection = connectionFactory.newConnection(channel, stateMachine);

        // complete handshake by notifying the client about the selected protocol revision
        ctx.writeAndFlush(new ProtocolNegotiationResponse(protocol.version()));

        // KeepAliveHandler needs the FrameSignalEncoder to send outbound NOOPs
        if (ctx.pipeline().get(KeepAliveHandler.class) != null) {
            ctx.pipeline().addBefore(KeepAliveHandler.NAME, "frameSignalEncoder", new FrameSignalEncoder());
        } else {
            ctx.pipeline().addLast(new FrameSignalEncoder());
        }

        ctx.channel()
                .config()
                .setWriteBufferWaterMark(new WriteBufferWaterMark(
                        config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_low_water_mark),
                        config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_high_water_mark)));

        ctx.pipeline()
                .addLast(
                        ChunkFrameDecoder.NAME,
                        new ChunkFrameDecoder(
                                config.get(
                                        BoltConnectorInternalSettings
                                                .unsupported_bolt_unauth_connection_max_inbound_bytes),
                                log))
                .addLast("chunkFrameEncoder", new ChunkFrameEncoder())
                .addLast("structDecoder", new PackstreamStructDecoder(protocol.requestMessageRegistry(connection), log))
                .addLast(
                        "structEncoder",
                        new PackstreamStructEncoder<>(
                                ResponseMessage.class, protocol.responseMessageRegistry(connection)))
                .addLast("goodbyeMessageHandler", new GoodbyeMessageHandler(connection, log))
                .addLast("resetMessageHandler", new ResetMessageHandler(connection, log))
                .addLast("boltStructEncoder", new BoltStructEncoder());

        var writeTimeoutMill = config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_max_duration)
                .toMillis();
        if (writeTimeoutMill != 0) {
            ctx.pipeline().addLast("channelThrottleHandler", new ChannelThrottleHandler(writeTimeoutMill));
        }

        ctx.pipeline()
                .addLast("outboundPayloadAccumulator", new RecordResponseAccumulator())
                .addLast("requestHandler", new RequestHandler(connection, new ResultHandler(connection, log)))
                .addLast("housekeeper", new HouseKeeperHandler(connection, logging.getLog(HouseKeeperHandler.class)))
                .remove(this);

        ctx.pipeline().remove(ProtocolNegotiationResponseEncoder.class);
        ctx.pipeline().remove(ProtocolNegotiationRequestDecoder.class);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Fatal error occurred during protocol handshaking: " + ctx.channel(), cause);
        ctx.close();
    }
}
