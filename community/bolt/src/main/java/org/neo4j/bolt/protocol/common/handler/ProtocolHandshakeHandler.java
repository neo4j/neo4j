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
package org.neo4j.bolt.protocol.common.handler;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.Arrays;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationRequestDecoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationResponseEncoder;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationRequest;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationResponse;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.codec.BoltStructEncoder;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector;
import org.neo4j.bolt.protocol.common.handler.messages.GoodbyeMessageHandler;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.runtime.throttle.ChannelReadThrottleHandler;
import org.neo4j.bolt.runtime.throttle.ChannelWriteThrottleHandler;
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

    private final InternalLogProvider logging;
    private final InternalLog log;

    private AbstractNettyConnector<?> connector;
    private Connection connection;

    public ProtocolHandshakeHandler(InternalLogProvider logging) {

        this.logging = logging;
        this.log = logging.getLog(getClass());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.connection = Connection.getConnection(ctx.channel());
        this.connector = (AbstractNettyConnector<?>) this.connection.connector();
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

            selectedProtocol = protocolRegistry.get(proposal).orElse(null);
        } while (selectedProtocol == null);

        // copy the final value to a separate variable as the compiler is otherwise incapable of identifying the value
        // as effectively final within this context
        var protocol = selectedProtocol;

        // complete handshake by notifying the connection about its new protocol revision and notify the peer about the
        // selected revision
        this.connection.selectProtocol(protocol);
        ctx.writeAndFlush(new ProtocolNegotiationResponse(protocol.version()));

        // KeepAliveHandler needs the FrameSignalEncoder to send outbound NOOPs
        ctx.pipeline()
                .addLast(new StateSignalFilterHandler())
                .addLast(new FrameSignalEncoder(protocol.frameSignalFilter()));

        var config = this.connector.configuration();
        if (config.enableOutboundBufferThrottle()) {
            ctx.channel()
                    .config()
                    .setWriteBufferWaterMark(new WriteBufferWaterMark(
                            config.outboundBufferThrottleLowWatermark(), config.outboundBufferThrottleHighWatermark()));
        }

        ChunkFrameDecoder frameDecoder;
        var readLimit = config.maxAuthenticationInboundBytes();
        if (readLimit != 0) {
            this.log.debug(
                    "Imposing %d byte read-limit on connection '%s' until authentication is completed",
                    readLimit, this.connection.id());
            frameDecoder = new ChunkFrameDecoder(readLimit, this.logging);
        } else {
            frameDecoder = new ChunkFrameDecoder(this.logging);
        }

        if (config.enableMergeCumulator()) {
            this.log.warn("Enabling merge cumulator for chunk decoding - Network performance may be degraded");
            frameDecoder.setCumulator(ByteToMessageDecoder.MERGE_CUMULATOR);
        }
        ctx.pipeline().addLast("chunkFrameDecoder", frameDecoder);

        // if raw protocol logging is enabled, we'll remove the previous handler and position it
        // after the chunk decoder handlers in order to split up the continuous byte stream into
        // coherent messages before passing them to the log
        if (config.enableProtocolLogging() && config.protocolLoggingMode().isLoggingRawTraffic()) {
            ctx.pipeline().remove(ProtocolLoggingHandler.RAW_NAME);
            ctx.pipeline().addLast(ProtocolLoggingHandler.RAW_NAME, new ProtocolLoggingHandler(this.logging));
        }

        ctx.pipeline()
                .addLast("chunkFrameEncoder", new ChunkFrameEncoder())
                .addLast(
                        "structDecoder",
                        new PackstreamStructDecoder<>(connection, protocol.requestMessageRegistry(), logging))
                .addLast(
                        "structEncoder",
                        new PackstreamStructEncoder<>(
                                ResponseMessage.class, connection, protocol.responseMessageRegistry()));

        var inboundMessageThrottleHighWatermark = config.inboundBufferThrottleHighWatermark();
        if (inboundMessageThrottleHighWatermark != 0) {
            ctx.pipeline()
                    .addLast(
                            "readThrottleHandler",
                            new ChannelReadThrottleHandler(
                                    config.inboundBufferThrottleLowWatermark(),
                                    inboundMessageThrottleHighWatermark,
                                    logging));
        }

        // if logging of decoded messages is enabled, we'll discard the old handler and introduce a
        // new instance at the correct position within the pipeline
        if (config.enableProtocolLogging() && config.protocolLoggingMode().isLoggingDecodedTraffic()) {
            ctx.pipeline().remove(ProtocolLoggingHandler.DECODED_NAME);
            ctx.pipeline().addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(this.logging));
        }

        ctx.pipeline()
                .addLast(GoodbyeMessageHandler.HANDLER_NAME, new GoodbyeMessageHandler(logging))
                .addLast("boltStructEncoder", new BoltStructEncoder());

        var writeThrottleEnabled = config.enableOutboundBufferThrottle();
        var writeTimeoutMillis = config.outboundBufferMaxThrottleDuration().toMillis();
        if (writeThrottleEnabled && writeTimeoutMillis != 0) {
            ctx.pipeline()
                    .addLast(
                            "channelThrottleHandler",
                            new ChannelWriteThrottleHandler(writeTimeoutMillis, this.logging));
        }

        ctx.pipeline()
                .addLast("requestHandler", new RequestHandler(logging))
                .addLast(HouseKeeperHandler.HANDLER_NAME, new HouseKeeperHandler(logging))
                .remove(this);

        ctx.pipeline().remove(ProtocolNegotiationResponseEncoder.class);
        ctx.pipeline().remove(ProtocolNegotiationRequestDecoder.class);

        this.connection.notifyListeners(listener -> listener.onProtocolSelected(protocol));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Fatal error occurred during protocol handshaking: " + ctx.channel(), cause);
        ctx.close();
    }
}
