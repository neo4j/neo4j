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

import static org.neo4j.bolt.protocol.common.handler.ProtocolHandshakeHandler.BOLT_MAGIC_PREAMBLE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.util.List;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationRequestDecoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationResponseEncoder;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionFactory;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.codec.transport.WebSocketFramePackingEncoder;
import org.neo4j.packstream.codec.transport.WebSocketFrameUnpackingDecoder;

public class TransportSelectionHandler extends ByteToMessageDecoder {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(TransportSelectionHandler.class);

    public static final long SSL_HANDLER_SHALLOW_SIZE = shallowSizeOfInstance(SslHandler.class);
    public static final long HTTP_SERVER_CODEC_SHALLOW_SIZE = shallowSizeOfInstance(HttpServerCodec.class);
    public static final long HTTP_OBJECT_AGGREGATOR_SHALLOW_SIZE = shallowSizeOfInstance(HttpObjectAggregator.class);
    public static final long WEB_SOCKET_SERVER_PROTOCOL_HANDLER_SHALLOW_SIZE =
            shallowSizeOfInstance(WebSocketServerProtocolHandler.class);
    public static final long WEB_SOCKET_FRAME_AGGREGATOR_SHALLOW_SIZE =
            shallowSizeOfInstance(WebSocketFrameAggregator.class);

    private static final String WEBSOCKET_MAGIC = "GET ";
    private static final int MAX_WEBSOCKET_HANDSHAKE_SIZE = 65536;
    private static final int MAX_WEBSOCKET_FRAME_SIZE = 65536;

    private final BoltChannel boltChannel;
    private final SslContext sslCtx;
    private final boolean encryptionRequired;
    private final boolean isEncrypted;
    private final InternalLogProvider logging;
    private final BoltConnectionFactory boltConnectionFactory;
    private final BoltProtocolRegistry boltProtocolRegistry;
    private final InternalLog log;
    private final DiscoveryResponseHandler discoveryResponseHandler;
    private final Config config;

    public TransportSelectionHandler(
            BoltChannel boltChannel,
            SslContext sslCtx,
            boolean encryptionRequired,
            boolean isEncrypted,
            InternalLogProvider logging,
            BoltConnectionFactory boltConnectionFactory,
            BoltProtocolRegistry boltProtocolRegistry,
            DiscoveryResponseHandler discoveryResponseHandler,
            Config config) {
        this.boltChannel = boltChannel;
        this.sslCtx = sslCtx;
        this.encryptionRequired = encryptionRequired;
        this.isEncrypted = isEncrypted;
        this.logging = logging;
        this.boltConnectionFactory = boltConnectionFactory;
        this.boltProtocolRegistry = boltProtocolRegistry;
        this.log = logging.getLog(TransportSelectionHandler.class);
        this.discoveryResponseHandler = discoveryResponseHandler;
        this.config = config;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Will use the first five bytes to detect a protocol.
        if (in.readableBytes() < 5) {
            return;
        }

        if (detectSsl(in)) {
            assertSslNotAlreadyConfigured(ctx);
            enableSsl(ctx);
        } else if (isHttp(in)) {
            switchToWebsocket(ctx);
        } else if (isBoltPreamble(in)) {
            switchToSocket(ctx);
        } else {
            // TODO: send a alert_message for a ssl connection to terminate the handshake
            in.clear();
            ctx.close();
        }
    }

    private void assertSslNotAlreadyConfigured(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        if (p.get(SslHandler.class) != null) {
            log.error(
                    "Fatal error: multiple levels of SSL encryption detected." + " Terminating connection: %s",
                    ctx.channel());
            ctx.close();
        }
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) {
        boltChannel.memoryTracker().releaseHeap(SHALLOW_SIZE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            // Netty throws a NativeIoException on connection reset - directly importing that class
            // caused a host of linking errors, because it depends on JNI to work. Hence, we just
            // test on the message we know we'll get.
            if (Exceptions.contains(cause, e -> e.getMessage().contains("Connection reset by peer"))) {
                log.warn(
                        "Fatal error occurred when initialising pipeline, "
                                + "remote peer unexpectedly closed connection: %s",
                        ctx.channel());
            } else {
                log.error("Fatal error occurred when initialising pipeline: " + ctx.channel(), cause);
            }
        } finally {
            ctx.close();
        }
    }

    private static boolean isBoltPreamble(ByteBuf in) {
        return in.getInt(0) == BOLT_MAGIC_PREAMBLE;
    }

    private boolean detectSsl(ByteBuf buf) {
        return sslCtx != null && SslHandler.isEncrypted(buf);
    }

    private static boolean isHttp(ByteBuf buf) {
        for (int i = 0; i < WEBSOCKET_MAGIC.length(); ++i) {
            if (buf.getUnsignedByte(buf.readerIndex() + i) != WEBSOCKET_MAGIC.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private void enableSsl(ChannelHandlerContext ctx) {
        // allocate sufficient space for another transport selection handlers as this instance will be freed upon
        // pipeline removal
        boltChannel.memoryTracker().allocateHeap(SSL_HANDLER_SHALLOW_SIZE + SHALLOW_SIZE);

        ctx.pipeline()
                .addLast(sslCtx.newHandler(ctx.alloc()))
                .addLast(new TransportSelectionHandler(
                        boltChannel,
                        null,
                        encryptionRequired,
                        true,
                        logging,
                        boltConnectionFactory,
                        boltProtocolRegistry,
                        discoveryResponseHandler,
                        config))
                .remove(this);
    }

    private void switchToSocket(ChannelHandlerContext ctx) {
        if (encryptionRequired && !isEncrypted) {
            throw new SecurityException("An unencrypted connection attempt was made where encryption is required.");
        }

        switchToHandshake(ctx);
    }

    private void switchToWebsocket(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();

        boltChannel
                .memoryTracker()
                .allocateHeap(HTTP_SERVER_CODEC_SHALLOW_SIZE
                        + HTTP_OBJECT_AGGREGATOR_SHALLOW_SIZE
                        + WEB_SOCKET_SERVER_PROTOCOL_HANDLER_SHALLOW_SIZE
                        + WEB_SOCKET_FRAME_AGGREGATOR_SHALLOW_SIZE
                        + WebSocketFramePackingEncoder.SHALLOW_SIZE
                        + WebSocketFrameUnpackingDecoder.SHALLOW_SIZE);

        p.addLast(
                new HttpServerCodec(),
                new HttpObjectAggregator(MAX_WEBSOCKET_HANDSHAKE_SIZE),
                discoveryResponseHandler,
                new WebSocketServerProtocolHandler("/", null, false, MAX_WEBSOCKET_FRAME_SIZE),
                new WebSocketFrameAggregator(MAX_WEBSOCKET_FRAME_SIZE),
                new WebSocketFramePackingEncoder(),
                new WebSocketFrameUnpackingDecoder());

        switchToHandshake(ctx);
    }

    private void switchToHandshake(ChannelHandlerContext ctx) {
        boltChannel
                .memoryTracker()
                .allocateHeap(ProtocolNegotiationResponseEncoder.SHALLOW_SIZE
                        + ProtocolNegotiationRequestDecoder.SHALLOW_SIZE
                        + ProtocolHandshakeHandler.SHALLOW_SIZE);

        ctx.pipeline()
                .addLast("protocolNegotiationRequestEncoder", new ProtocolNegotiationResponseEncoder())
                .addLast("protocolNegotiationRequestDecoder", new ProtocolNegotiationRequestDecoder())
                .addLast(
                        "protocolHandshakeHandler",
                        new ProtocolHandshakeHandler(
                                boltProtocolRegistry, boltConnectionFactory, boltChannel, logging, config));

        ctx.pipeline().remove(this);
    }
}
