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

import static org.neo4j.bolt.protocol.common.handler.ProtocolHandshakeHandler.BOLT_MAGIC_PREAMBLE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.List;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationRequestDecoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationResponseEncoder;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.codec.transport.WebSocketFramePackingEncoder;
import org.neo4j.packstream.codec.transport.WebSocketFrameUnpackingDecoder;
import org.neo4j.util.VisibleForTesting;

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

    private final InternalLogProvider logging;
    private final InternalLog log;

    private final boolean isEncrypted;

    private AbstractNettyConnector<?> connector;
    private Connection connection;

    @VisibleForTesting
    TransportSelectionHandler(boolean isEncrypted, InternalLogProvider logging) {
        this.isEncrypted = isEncrypted;

        this.logging = logging;
        this.log = logging.getLog(TransportSelectionHandler.class);
    }

    public TransportSelectionHandler(InternalLogProvider logging) {
        this(false, logging);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.connection = Connection.getConnection(ctx.channel());
        this.connector = (AbstractNettyConnector<?>) this.connection.connector();
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) {
        this.connection.memoryTracker().releaseHeap(SHALLOW_SIZE);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Will use the first five bytes to detect a protocol.
        if (in.readableBytes() < 5) {
            return;
        }

        if (detectSsl(in)) {
            if (this.isEncrypted) {
                log.error(
                        "Fatal error: multiple levels of SSL encryption detected." + " Terminating connection: %s",
                        ctx.channel());
                ctx.close();

                return;
            }

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
        return this.connector.configuration().sslContext() != null && SslHandler.isEncrypted(buf);
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
        // perform TLS negotiation asynchronously before switching back to transport selection once
        // a secure channel has been established
        connection.memoryTracker().allocateHeap(SSL_HANDLER_SHALLOW_SIZE);

        var config = this.connector.configuration();
        var sslContext = config.sslContext();
        var handler = sslContext.newHandler(ctx.alloc());

        handler.handshakeFuture()
                .addListener(new TransportSecuritySelectionFutureListener(
                        ctx.channel(), this.connection, this.logging, this.log));

        // discard this handler distance in order to allow SslHandler to negotiate a secure
        // connection with the peer - we will reattach once the connection has successfully been
        // secured
        ctx.pipeline().addLast(handler).remove(this);
    }

    private void switchToSocket(ChannelHandlerContext ctx) {
        if (this.connector.configuration().requiresEncryption() && !isEncrypted) {
            throw new SecurityException("An unencrypted connection attempt was made where encryption is required.");
        }

        switchToHandshake(ctx);
    }

    private void switchToWebsocket(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();

        connection
                .memoryTracker()
                .allocateHeap(HTTP_SERVER_CODEC_SHALLOW_SIZE
                        + HTTP_OBJECT_AGGREGATOR_SHALLOW_SIZE
                        + DiscoveryResponseHandler.SHALLOW_SIZE
                        + WEB_SOCKET_SERVER_PROTOCOL_HANDLER_SHALLOW_SIZE
                        + WEB_SOCKET_FRAME_AGGREGATOR_SHALLOW_SIZE
                        + WebSocketFramePackingEncoder.SHALLOW_SIZE
                        + WebSocketFrameUnpackingDecoder.SHALLOW_SIZE);

        p.addLast(
                new HttpServerCodec(),
                new HttpObjectAggregator(MAX_WEBSOCKET_HANDSHAKE_SIZE),
                new DiscoveryResponseHandler(this.connector.authConfigProvider()),
                new WebSocketServerProtocolHandler("/", null, false, MAX_WEBSOCKET_FRAME_SIZE),
                new WebSocketFrameAggregator(MAX_WEBSOCKET_FRAME_SIZE),
                new WebSocketFramePackingEncoder(),
                new WebSocketFrameUnpackingDecoder());

        switchToHandshake(ctx);
    }

    private void switchToHandshake(ChannelHandlerContext ctx) {
        var config = this.connector.configuration();

        // if logging of raw traffic has been enabled, we'll attach a new protocol logging handler
        // now in order to capture messages before they enter the negotiation and Packstream decoder
        // pipelines
        if (config.enableProtocolLogging() && config.protocolLoggingMode().isLoggingRawTraffic()) {
            connection.memoryTracker().allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);
            ctx.pipeline().addLast(ProtocolLoggingHandler.RAW_NAME, new ProtocolLoggingHandler(this.logging));
        }

        connection
                .memoryTracker()
                .allocateHeap(ProtocolNegotiationResponseEncoder.SHALLOW_SIZE
                        + ProtocolNegotiationRequestDecoder.SHALLOW_SIZE
                        + ProtocolHandshakeHandler.SHALLOW_SIZE);

        ctx.pipeline()
                .addLast("protocolNegotiationRequestEncoder", new ProtocolNegotiationResponseEncoder())
                .addLast("protocolNegotiationRequestDecoder", new ProtocolNegotiationRequestDecoder());

        // if logging of decoded messages is enabled, we'll also attach another separate decoding
        // handler in order to capture negotiation requests and responses during this protocol phase
        if (config.enableProtocolLogging() && config.protocolLoggingMode().isLoggingDecodedTraffic()) {
            connection.memoryTracker().allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);
            ctx.pipeline().addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(this.logging));
        }

        ctx.pipeline().addLast("protocolHandshakeHandler", new ProtocolHandshakeHandler(logging));

        ctx.pipeline().remove(this);
    }

    /**
     * Encapsulates the logic necessary to perform transport selection once TLS has been negotiated.
     * <p />
     * This implementation is separated into a dedicated listener implementation to ensure that we
     * do not accidentally keep a strong reference to {@link TransportSelectionHandler} during the
     * TLS handshake.
     */
    private static class TransportSecuritySelectionFutureListener
            implements GenericFutureListener<Future<? super Channel>> {
        private final Channel channel;
        private final Connection connection;
        private final InternalLogProvider logging;
        private final Log log;

        public TransportSecuritySelectionFutureListener(
                Channel channel, Connection connection, InternalLogProvider logging, Log log) {
            this.channel = channel;
            this.connection = connection;
            this.logging = logging;
            this.log = log;
        }

        @Override
        public void operationComplete(Future<? super Channel> f) throws Exception {
            if (!f.isSuccess()) {
                var cause = f.cause();
                var message = "Unknown Error";
                if (cause != null) {
                    message = cause.getMessage();
                }

                log.debug("[%s] TLS handshake has failed: %s", this.channel.remoteAddress(), message);

                // SslHandler likely closes the connection as well, but we make sure that it does
                // not remain active even if netty behavior changes in the future
                this.channel.close();
                return;
            }

            // as of now we are on an encrypted channel where SslHandler will take care of the en-
            // and decryption of outgoing and incoming data thus permitting us to continue the
            // application protocol selection as usual
            connection.memoryTracker().allocateHeap(SHALLOW_SIZE);

            this.channel.pipeline().addLast(new TransportSelectionHandler(true, this.logging));
        }
    }
}
