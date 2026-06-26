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
package org.neo4j.bolt.testing.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.negotiation.util.BitMask;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.testing.client.error.BoltTestClientClosedException;
import org.neo4j.bolt.testing.client.error.BoltTestClientConnectionTimeoutException;
import org.neo4j.bolt.testing.client.error.BoltTestClientException;
import org.neo4j.bolt.testing.client.error.BoltTestClientIOException;
import org.neo4j.bolt.testing.client.error.BoltTestClientInterruptedException;
import org.neo4j.bolt.testing.client.error.BoltTestClientReadTimeoutException;
import org.neo4j.bolt.testing.client.error.BoltTestClientStateException;
import org.neo4j.bolt.testing.client.error.BoltTestClientWriteTimeoutException;
import org.neo4j.bolt.testing.client.handler.NotifyingChannelInboundHandler;
import org.neo4j.bolt.testing.client.handler.NotifyingChannelResponseMessageInboundHandler;
import org.neo4j.bolt.testing.client.handler.TestChannelInitializer;
import org.neo4j.bolt.testing.client.struct.ProtocolProposal;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.boltmessages.request.RequestMessage;
import org.neo4j.boltmessages.response.ResponseMessage;
import org.neo4j.internal.helpers.Exceptions;

public abstract sealed class AbstractNettyConnection implements BoltTestConnection, UnwiredTestConnection
        permits LocalConnection, SocketConnection, UnixDomainSocketConnection {

    private static final int MAX_CHUNK_SIZE = 1 << 16 - 1;

    protected static final String LOGGING_HANDLER_NAME = "loggingHandler";
    protected static final String INBOUND_HANDLER_NAME = "notifyingChannelInboundHandler";
    public static final long RECV_TIMEOUT = 30_000_000_000L;
    public static final int READ_LOCK_TIMEOUT = 1_000;

    protected final ConnectorTransport transport;
    protected final BoltWire wire;
    private final EventLoopGroup eventLoopGroup;
    protected final Object readLock = new Object();
    protected final CompositeByteBuf readBuffer = Unpooled.compositeBuffer();
    protected final List<ResponseMessage> responseMessageList;

    private boolean closed;
    private Channel channel;
    protected volatile SSLEngine sslEngine;

    protected final Map<ChannelOption, Object> options = new HashMap<>();
    protected X509Certificate certificate;
    protected PrivateKey privateKey;

    private long noopCount;

    public AbstractNettyConnection(ConnectorTransport transport, BoltWire wire) {
        this.transport = transport;
        this.wire = wire;
        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(1, transport.createIoHandlerFactory());
        this.responseMessageList = Collections.synchronizedList(new ArrayList<>());
    }

    protected void ensureValid() {
        if (this.closed) {
            throw new IllegalStateException("Test client has already been closed");
        }
    }

    protected abstract SocketAddress address();

    protected abstract Class<? extends Channel> channelType();

    protected void customizeBootstrap(Bootstrap bootstrap) {
        // NOOP
    }

    protected ChannelPromise initializeChannel(Channel ch) {
        Future<Channel> future = null;
        try {
            var sslContext = this.sslContext();
            if (sslContext != null) {
                // Note: Hostname verification is deliberately absent from this snippet as our tests
                // have no need for this at the moment
                var handler = sslContext.newHandler(UnpooledByteBufAllocator.DEFAULT);
                future = handler.handshakeFuture();

                this.sslEngine = handler.engine();

                ch.pipeline().addLast(handler);
            } else if (this.certificate != null) {
                throw new IllegalStateException("Requested mTLS authentication on connection without TLS support");
            }
        } catch (SSLException ex) {
            throw new BoltTestClientConnectionTimeoutException("Failed to instantiate SslContext", ex);
        }

        ch.pipeline()
                .addLast(LOGGING_HANDLER_NAME, new LoggingHandler(LogLevel.INFO))
                .addLast(INBOUND_HANDLER_NAME, getNotifyingChannelInboundHandler());

        var promise = ch.newPromise();
        if (future == null) {
            promise.setSuccess();
        } else {
            future.addListener(f -> {
                if (f.isSuccess()) {
                    promise.setSuccess();
                } else {
                    promise.setFailure(f.cause());
                }
            });
        }
        return promise;
    }

    protected SslContext sslContext() throws SSLException {
        // disabled by default
        return null;
    }

    protected void ensureActive() {
        if (this.channel == null || !this.channel.isActive()) {
            throw new BoltTestClientClosedException("Connection closed");
        }
    }

    @Override
    public BoltWire wire() {
        this.ensureValid();
        return this.wire;
    }

    @Override
    public BoltTestConnection connect() throws BoltTestClientException {
        this.ensureValid();

        if (this.channel != null && this.channel.isOpen()) {
            return this;
        }

        var address = this.address();
        var initializer = new TestChannelInitializer(this::initializeChannel);

        var bootstrap = new Bootstrap()
                .group(this.eventLoopGroup)
                .channel(this.channelType())
                .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
                .handler(initializer);

        for (var entry : this.options.entrySet()) {
            bootstrap.option(entry.getKey(), entry.getValue());
        }

        this.customizeBootstrap(bootstrap);

        try {
            var f = bootstrap.connect(address);
            if (!f.await(30, TimeUnit.SECONDS)) {
                f.cancel(true);

                throw new BoltTestClientConnectionTimeoutException(
                        "Failed to establish connection to " + address + ": Timed out after 30 seconds");
            }

            if (!f.isSuccess()) {
                throw new BoltTestClientClosedException("Failed to establish connection: " + address, f.cause());
            }

            this.channel = f.channel();

            initializer.awaitInitialization();
        } catch (InterruptedException ex) {
            throw new BoltTestClientInterruptedException(ex);
        }
        return this;
    }

    @Override
    public BoltTestConnection setCertificate(X509Certificate certificate, PrivateKey privateKey) {
        this.ensureValid();

        Objects.requireNonNull(certificate);
        Objects.requireNonNull(privateKey);

        this.certificate = certificate;
        this.privateKey = privateKey;
        return this;
    }

    @Override
    public <T> BoltTestConnection setOption(ChannelOption<T> option, T value) {
        this.ensureValid();

        this.options.put(option, value);

        if (this.channel != null) {
            this.channel.config().setOption(option, value);
        }

        return null;
    }

    @Override
    public BoltTestConnection disconnect() {
        this.ensureValid();

        if (this.channel == null) {
            return this;
        }

        try {
            if (this.channel.isOpen()) {
                try {
                    var f = this.channel.close();
                    f.await();

                    if (!f.isSuccess()) {
                        throw new BoltTestClientClosedException(
                                "Failed to close channel: " + this.channel.remoteAddress(), f.cause());
                    }
                } catch (InterruptedException ex) {
                    throw new BoltTestClientInterruptedException(ex);
                }
            }
        } finally {
            this.channel = null;
            this.sslEngine = null;
        }

        return this;
    }

    @Override
    public BoltTestConnection sendRaw(ByteBuf buf) {
        this.ensureValid();

        if (this.channel == null) {
            throw new BoltTestClientStateException("No active connection");
        }

        this.ensureActive();

        try {
            var f = this.channel.writeAndFlush(buf);
            if (!f.await(30, TimeUnit.SECONDS)) {
                f.cancel(true);

                this.ensureActive();

                throw new BoltTestClientWriteTimeoutException(
                        "Failed to write message to " + this.channel.remoteAddress() + ": Timed out after 30 seconds");
            }

            if (!f.isSuccess()) {
                var cause = f.cause();

                var networkErrors = Exceptions.contains(cause, e -> {
                    var simpleName = e.getClass().getSimpleName();
                    if (simpleName.contains("StacklessClosedChannel") || simpleName.contains("NativeIoException")) {
                        return true;
                    }

                    return e.getMessage() != null && e.getMessage().contains("Connection reset by peer");
                });

                if (networkErrors) {
                    throw new BoltTestClientClosedException(cause);
                }

                throw new BoltTestClientClosedException(
                        "Failed to write message to " + this.channel.remoteAddress(), f.cause());
            }
        } catch (InterruptedException ex) {
            throw new BoltTestClientInterruptedException(ex);
        }
        return this;
    }

    @Override
    public BoltTestConnection send(
            ProtocolVersion version1, ProtocolVersion version2, ProtocolVersion version3, ProtocolVersion version4) {
        return this.sendRaw(Unpooled.buffer(20)
                .writeInt(0x6060B017)
                .writeInt(version1.encode())
                .writeInt(version2.encode())
                .writeInt(version3.encode())
                .writeInt(version4.encode()));
    }

    @Override
    public BoltTestConnection send(ProtocolVersion version, Set<ProtocolCapability> capabilities) throws IOException {
        var buffer = Unpooled.buffer().writeInt(version.encode());

        writeBitMask(buffer, ProtocolCapability.toBitMask(buffer.alloc(), capabilities));

        return this.sendRaw(buffer);
    }

    @Override
    public void unwired(Consumer<UnwiredTestConnection> work) {
        this.ensureValid();

        this.channel.pipeline().remove(INBOUND_HANDLER_NAME);
        this.channel
                .pipeline()
                .addLast(new NotifyingChannelResponseMessageInboundHandler(this.responseMessageList, this.readLock));
        work.accept(this);
    }

    private NotifyingChannelInboundHandler getNotifyingChannelInboundHandler() {
        return new NotifyingChannelInboundHandler(this.readBuffer, this.readLock);
    }

    @Override
    public UnwiredTestConnection sendRequest(RequestMessage requestMessage) {
        this.ensureValid();

        if (this.channel == null) {
            throw new BoltTestClientStateException("No active connection");
        }

        this.ensureActive();

        try {
            var f = this.channel.writeAndFlush(requestMessage);
            if (!f.await(30, TimeUnit.SECONDS)) {
                f.cancel(true);

                this.ensureActive();

                throw new BoltTestClientWriteTimeoutException(
                        "Failed to write message to " + this.channel.remoteAddress() + ": Timed out after 30 seconds");
            }

            if (!f.isSuccess()) {
                var cause = f.cause();

                var networkErrors = Exceptions.contains(cause, e -> {
                    var simpleName = e.getClass().getSimpleName();
                    if (simpleName.contains("StacklessClosedChannel") || simpleName.contains("NativeIoException")) {
                        return true;
                    }

                    return e.getMessage() != null && e.getMessage().contains("Connection reset by peer");
                });

                if (networkErrors) {
                    throw new BoltTestClientClosedException(cause);
                }

                throw new BoltTestClientClosedException(
                        "Failed to write message to " + this.channel.remoteAddress(), f.cause());
            }
        } catch (InterruptedException ex) {
            throw new BoltTestClientInterruptedException(ex);
        }

        return this;
    }

    @Override
    public ResponseMessage receiveResponse() {
        this.ensureValid();

        if (this.channel == null) {
            throw new BoltTestClientStateException("No active connection");
        }

        synchronized (this.readLock) {
            var readInitializedAt = System.nanoTime();
            while (this.responseMessageList.isEmpty()) {
                this.ensureActive();

                try {
                    this.readLock.wait(READ_LOCK_TIMEOUT);

                    // abort if the message has not been made available within a reasonable amount
                    // of time
                    if (this.responseMessageList.isEmpty()) {
                        var currentTime = System.nanoTime();
                        if (currentTime - readInitializedAt > RECV_TIMEOUT) {
                            throw new BoltTestClientReadTimeoutException("Timeout reading next message");
                        }
                    }
                } catch (InterruptedException ex) {
                    throw new BoltTestClientInterruptedException(ex);
                }
            }
            return this.responseMessageList.removeFirst();
        }
    }

    private static void writeBitMask(ByteBuf buf, BitMask mask) {
        var totalBits = mask.length();
        var encodedLength = totalBits / 7 + (totalBits % 7 == 0 ? 0 : 1);

        for (var i = 0; i < encodedLength; i++) {
            var b = mask.readN(Math.min(7, mask.readable()));
            if (i + 1 < encodedLength) {
                b ^= 0x80;
            }

            buf.writeByte(b);
        }
    }

    @Override
    public BoltTestConnection send(ByteBuf buf) {
        do {
            var length = Math.min(buf.readableBytes(), MAX_CHUNK_SIZE);

            var bytes = buf.readBytes(length);

            var header = Unpooled.buffer(2);
            header.writeShort(length);

            this.sendRaw(Unpooled.compositeBuffer(2).addComponent(true, header).addComponent(true, bytes));

            if (length == 0) {
                buf.release();
                return this;
            }
        } while (buf.isReadable());

        this.sendRaw(Unpooled.buffer(2).writeShort(0));
        buf.release();
        return this;
    }

    @Override
    public long noopCount() {
        this.ensureValid();

        return this.noopCount;
    }

    @Override
    public ByteBuf receive(int length) {
        this.ensureValid();

        // buffer is deliberately unpooled in order to keep the test code as simple as
        // possible (performance being secondary here)
        var buf = Unpooled.buffer(length);

        synchronized (this.readLock) {
            var readInitializedAt = System.nanoTime();
            var currentReadableBytes = this.readBuffer.readableBytes();
            while (currentReadableBytes < length) {
                this.ensureActive();

                try {
                    this.readLock.wait(READ_LOCK_TIMEOUT);
                    currentReadableBytes = this.readBuffer.readableBytes();

                    // abort if the message has not been made available within a reasonable amount
                    // of time
                    if (currentReadableBytes < length) {
                        var currentTime = System.nanoTime();
                        if (currentTime - readInitializedAt > RECV_TIMEOUT) {
                            var message = "Failed to receive expected message of " + length
                                    + " bytes within deadline of 30 seconds (available bytes: " + currentReadableBytes
                                    + "; channel: " + (this.channel.isOpen() ? "open" : "closed") + ")";

                            throw new BoltTestClientReadTimeoutException(message);
                        }
                    }
                } catch (InterruptedException ex) {
                    throw new BoltTestClientInterruptedException(ex);
                }
            }

            this.readBuffer.readBytes(buf, length);
            this.readBuffer.discardReadComponents();
        }

        return buf;
    }

    @Override
    public ProtocolVersion receiveNegotiatedVersion() {
        return new ProtocolVersion(this.receive(ProtocolVersion.ENCODED_SIZE).readInt());
    }

    @Override
    public ProtocolProposal receiveProtocolProposal() {
        var negotiationVersion = new ProtocolVersion(this.receive(4).readInt());

        var versionLength = this.receiveVarInt();
        if (versionLength < 0) {
            throw new AssertionError("Received illegal protocol proposal: Announced " + versionLength + " versions");
        }

        var versions = new ArrayList<ProtocolVersion>(versionLength);
        for (var i = 0; i < versionLength; ++i) {
            versions.add(new ProtocolVersion(this.receive(4).readInt()));
        }

        var capabilityMask = this.receiveBitMask();
        var capabilities = ProtocolCapability.fromBitMask(capabilityMask);

        return new ProtocolProposal(negotiationVersion, versions, capabilities);
    }

    @Override
    public int receiveVarInt() {
        var value = 0;
        for (var i = 0; i < 5; ++i) {
            var b = this.receive(1).readUnsignedByte();
            value ^= (b & 0x7F) << (7 * i);

            if ((b & 0x80) == 0) {
                return value;
            }
        }

        throw new AssertionError("Received illegal VarInt consisting of more than 5 bytes");
    }

    public BitMask receiveBitMask() {
        var recv = Unpooled.buffer();

        byte i;
        do {
            i = this.receive(1).readByte();
            recv.writeByte(i);
        } while ((i & 0x80) != 0x00);

        var mask = new BitMask(recv.alloc(), recv.readableBytes() * 7);

        do {
            var b = recv.readByte();
            mask.writeN(b, 7);
        } while (recv.isReadable());

        return mask;
    }

    @Override
    public int receiveChunkHeader() {
        return this.receive(2).readUnsignedShort();
    }

    @Override
    public ByteBuf receiveMessage() {
        var noopCount = 0L;
        var composite = Unpooled.compositeBuffer();
        while (true) {
            var chunkLength = this.receiveChunkHeader();

            if (chunkLength == 0) {
                // ignore NOOPs
                if (composite.numComponents() == 0) {
                    noopCount++;
                    continue;
                }

                this.noopCount = noopCount;
                return composite;
            }

            composite.addComponent(true, this.receive(chunkLength));
        }
    }

    @Override
    public boolean isDisconnected() {
        this.ensureValid();

        try {
            this.sendRaw(new byte[] {0, 0});
            return !this.channel.isActive();
        } catch (BoltTestClientIOException e) {
            return true;
        }
    }

    @Override
    public void close() {
        try {
            BoltTestConnection.super.close();
        } finally {
            ReferenceCountUtil.safeRelease(this.readBuffer);
            this.closed = true;
        }
    }
}
