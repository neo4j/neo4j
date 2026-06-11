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

import static org.neo4j.bolt.negotiation.handler.LegacyProtocolHandshakeHandler.BOLT_MAGIC_PREAMBLE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationRequestDecoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationResponseEncoder;
import org.neo4j.bolt.negotiation.handler.LegacyProtocolHandshakeHandler;
import org.neo4j.bolt.protocol.common.connector.config.ExternalConnectorConfiguration;
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

    /**
     * Channel attribute key to track PROXY protocol processing state.
     * Values: null (not checked), Boolean.TRUE (PROXY protocol detected and processing), Boolean.FALSE (no PROXY protocol)
     */
    static final AttributeKey<Boolean> PROXY_PROTOCOL_PROCESSING =
            AttributeKey.valueOf(TransportSelectionHandler.class, "proxyProtocolProcessing");

    // PROXY protocol v1 signature: "PROXY " (text-based)
    private static final byte[] PROXY_V1_PREFIX = "PROXY ".getBytes(StandardCharsets.US_ASCII);

    // PROXY protocol v2 signature: 12-byte binary header
    private static final byte[] PROXY_V2_PREFIX = {
        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A
    };

    // Minimum bytes needed for PROXY protocol detection (v1 is shorter)
    private static final int MIN_PROXY_DETECTION_BYTES = PROXY_V1_PREFIX.length;
    // Maximum bytes needed for PROXY protocol detection (v2 is longer)
    private static final int MAX_PROXY_DETECTION_BYTES = PROXY_V2_PREFIX.length;

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
        // First, check if PROXY protocol processing is enabled and handle it if needed
        var proxyProcessing = ctx.channel().attr(PROXY_PROTOCOL_PROCESSING).get();
        if (proxyProcessing == null && this.connector.configuration().enableProxyProtocol()) {
            // Need at least minimum bytes to check for PROXY protocol v1
            if (in.readableBytes() < MIN_PROXY_DETECTION_BYTES) {
                // Not enough bytes yet to check for PROXY protocol
                return;
            }

            // Check for PROXY protocol v1 first (only needs 6 bytes)
            boolean isProxyV1 = in.readableBytes() >= PROXY_V1_PREFIX.length && startsWithPrefix(in, PROXY_V1_PREFIX);

            // If we have enough bytes, also check for PROXY protocol v2 (needs 12 bytes)
            boolean isProxyV2 = false;
            if (in.readableBytes() >= PROXY_V2_PREFIX.length) {
                isProxyV2 = startsWithPrefix(in, PROXY_V2_PREFIX);
            }

            if (isProxyV1 || isProxyV2) {
                // PROXY protocol detected - install decoders and mark as processing
                log.debug("[%s] PROXY protocol header detected, installing decoder", connection.id());
                installProxyProtocolHandlers(ctx);
                ctx.channel().attr(PROXY_PROTOCOL_PROCESSING).set(Boolean.TRUE);
                log.debug("Proxy Protocol detection: true");
                // Pass all bytes to the next handler (HAProxyMessageDecoder) so it can decode the header
                ByteBuf toFire = in.retainedDuplicate();
                in.readerIndex(in.writerIndex());
                ctx.executor().execute(() -> ctx.pipeline().fireChannelRead(toFire));
                return;
            }

            // If we don't have enough bytes to check v2 (12 bytes), we need to decide:
            // - If we have enough for transport detection (5 bytes), we can proceed
            //   (it's very unlikely to be PROXY v2 if it doesn't match v1)
            // - Otherwise, wait for more bytes
            if (in.readableBytes() < MAX_PROXY_DETECTION_BYTES) {
                if (in.readableBytes() < 5) {
                    // Not enough bytes for either PROXY v2 detection or transport detection - wait
                    return;
                }
                // We have enough for transport detection but not PROXY v2 - proceed with transport selection
                // (if it were PROXY v2, the first 6 bytes would match v1 pattern, which they don't)
            }

            // No PROXY protocol detected - mark as checked and continue with normal transport selection
            ctx.channel().attr(PROXY_PROTOCOL_PROCESSING).set(Boolean.FALSE);
            log.debug("Proxy Protocol detection: false");
            // Continue to normal transport selection below
        } else if (Boolean.TRUE.equals(proxyProcessing)) {
            // PROXY protocol is being processed - wait for it to complete
            // The HAProxyInfoExtractor will set this to false when done
            return;
        }

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
        var preamble = in.getInt(in.readerIndex());
        return preamble == BOLT_MAGIC_PREAMBLE;
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

    /**
     * Checks if the buffer starts with the given prefix without consuming bytes.
     */
    private static boolean startsWithPrefix(ByteBuf buf, byte[] prefix) {
        for (int i = 0; i < prefix.length; i++) {
            if (buf.getByte(buf.readerIndex() + i) != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Installs the PROXY protocol decoder and info extractor into the pipeline.
     */
    private void installProxyProtocolHandlers(ChannelHandlerContext ctx) {
        // Account for memory usage
        connection
                .memoryTracker()
                .allocateHeap(HeapEstimator.shallowSizeOfInstance(HAProxyMessageDecoder.class)
                        + HAProxyInfoExtractor.SHALLOW_SIZE);

        // Add decoder and extractor to pipeline before this handler
        // They will process the PROXY header bytes and then pass through remaining bytes
        // ctx.pipeline()
        //        .addAfter(ctx.name(), "haproxyExtractor", new HAProxyInfoExtractor(this.logging))
        //        .addAfter(ctx.name(), "haproxyDecoder", new HAProxyMessageDecoder());
        ctx.pipeline()
                .addBefore(ctx.name(), "haproxyDecoder", new HAProxyMessageDecoder())
                .addBefore(ctx.name(), "haproxyExtractor", new HAProxyInfoExtractor(this.logging));
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
        assertEncryption();

        switchToHandshake(ctx);
    }

    private void assertEncryption() {
        if (!isEncrypted
                && this.connector.configuration() instanceof ExternalConnectorConfiguration externalConfig
                && externalConfig.requireEncryption()) {
            throw new SecurityException("An unencrypted connection attempt was made where encryption is required.");
        }
    }

    private void switchToWebsocket(ChannelHandlerContext ctx) {
        assertEncryption();

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
                        + LegacyProtocolHandshakeHandler.SHALLOW_SIZE);

        ctx.pipeline()
                .addLast("protocolNegotiationRequestEncoder", new ProtocolNegotiationResponseEncoder())
                .addLast("protocolNegotiationRequestDecoder", new ProtocolNegotiationRequestDecoder());

        // if logging of decoded messages is enabled, we'll also attach another separate decoding
        // handler in order to capture negotiation requests and responses during this protocol phase
        if (config.enableProtocolLogging() && config.protocolLoggingMode().isLoggingDecodedTraffic()) {
            connection.memoryTracker().allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);
            ctx.pipeline().addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(this.logging));
        }

        ctx.pipeline().addLast("protocolHandshakeHandler", new LegacyProtocolHandshakeHandler(logging));

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
