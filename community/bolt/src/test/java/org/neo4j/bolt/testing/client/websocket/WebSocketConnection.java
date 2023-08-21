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
package org.neo4j.bolt.testing.client.websocket;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.neo4j.bolt.testing.client.AbstractTransportConnection;
import org.neo4j.bolt.testing.client.TransportConnection;

public class WebSocketConnection extends AbstractTransportConnection implements WebSocketListener {
    private static final Factory factory = new Factory();

    private static final byte[] POISON_PILL = "poison".getBytes();

    private final SocketAddress address;

    private WebSocketClient client;
    private RemoteEndpoint server;

    // Incoming data goes on this queue
    private final LinkedBlockingQueue<byte[]> received = new LinkedBlockingQueue<>();

    // Current input data being handled, popped off of 'received' queue
    private byte[] currentReceiveBuffer;

    // Index into the current receive buffer
    private int currentReceiveIndex;

    public static TransportConnection.Factory factory() {
        return factory;
    }

    public WebSocketConnection(SocketAddress address) {
        requireNonNull(address);

        this.address = address;
    }

    protected WebSocketClient createClient() {
        return new WebSocketClient();
    }

    protected URI createTargetUri(SocketAddress address) {
        var socketAddress = (InetSocketAddress) address;
        return URI.create("ws://" + socketAddress.getHostString() + ":" + socketAddress.getPort());
    }

    private void ensureConnected() {
        if (this.client == null || this.server == null) {
            throw new IllegalStateException(
                    "Client has not established a connection - Make sure your test is configured correctly");
        }
    }

    @Override
    public TransportConnection connect() throws IOException {
        if (this.client != null && !this.client.isStopped()) {
            return this;
        }

        this.client = this.createClient();
        try {
            client.start();
        } catch (Exception e) {
            throw new IOException("Failed to start WebSocket client", e);
        }

        Session session;
        try {
            var uri = this.createTargetUri(this.address);
            session = this.client.connect(this, uri).get(5, MINUTES);
        } catch (Exception e) {
            throw new IOException("Failed to connect to the server within 5 minutes", e);
        }

        server = session.getRemote();
        return this;
    }

    @Override
    public TransportConnection sendRaw(byte[] rawBytes) throws IOException {
        this.ensureConnected();

        // The WS client *mutates* the buffer we give it, so we need to copy it here to allow the caller to retain
        // ownership
        ByteBuffer wrap = ByteBuffer.wrap(Arrays.copyOf(rawBytes, rawBytes.length));
        server.sendBytes(wrap);
        return this;
    }

    @Override
    public TransportConnection sendRaw(ByteBuf buf) throws IOException {
        this.ensureConnected();

        var heap = ByteBuffer.allocate(128);
        do {
            heap.flip();
            heap.limit(Math.min(buf.readableBytes(), heap.capacity()));

            buf.readBytes(heap);
            heap.flip();
            this.server.sendBytes(heap);
        } while (buf.isReadable());

        return this;
    }

    @Override
    public ByteBuf receive(int length) throws IOException, InterruptedException {
        this.ensureConnected();

        var buffer = Unpooled.buffer(length, length);

        while (buffer.isWritable()) {
            try {
                waitForReceivedData(buffer.writableBytes());
            } catch (IOException ex) {
                throw new IOException(
                        "Failed to retrieve message - Remaining buffer content: " + ByteBufUtil.hexDump(buffer), ex);
            }

            for (int i = 0;
                    i < Math.min(buffer.writableBytes(), currentReceiveBuffer.length - currentReceiveIndex);
                    i++) {
                buffer.writeByte(currentReceiveBuffer[currentReceiveIndex++]);
            }
        }

        return buffer;
    }

    private void waitForReceivedData(int remaining) throws InterruptedException, IOException {
        long start = System.currentTimeMillis();
        while (currentReceiveBuffer == null || currentReceiveIndex >= currentReceiveBuffer.length) {
            currentReceiveIndex = 0;
            currentReceiveBuffer = received.poll(10, MILLISECONDS);

            if ((currentReceiveBuffer == null && (client.isStopped() || client.isStopping()))
                    || currentReceiveBuffer == POISON_PILL) {
                // no data received
                throw new IOException("Connection closed while waiting for data from the server.");
            }
            if (System.currentTimeMillis() - start > 60_000) {
                throw new IOException("Waited 60 seconds for " + remaining + " bytes");
            }
        }
    }

    @Override
    public TransportConnection disconnect() throws IOException {
        if (this.client == null) {
            return this;
        }

        try {
            this.client.stop();
            currentReceiveBuffer = null;
            received.clear();
        } catch (Exception ex) {
            throw new IOException("Failed to terminate connection cleanly", ex);
        } finally {
            this.client = null;
            this.server = null;
        }

        return this;
    }

    @Override
    public void onWebSocketBinary(byte[] bytes, int i, int i2) {
        received.add(bytes);
    }

    @Override
    public void onWebSocketClose(int i, String s) {
        received.add(POISON_PILL);
    }

    @Override
    public void onWebSocketConnect(Session session) {}

    @Override
    public void onWebSocketError(Throwable throwable) {}

    @Override
    public void onWebSocketText(String s) {}

    private static class Factory implements TransportConnection.Factory {

        @Override
        public TransportConnection create(SocketAddress address) {
            return new WebSocketConnection(address);
        }

        @Override
        public String toString() {
            return "Plain WebSocket";
        }
    }
}
