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

import static java.util.Objects.requireNonNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class UnixDomainSocketConnection extends AbstractTransportConnection {

    private static final Factory factory = new Factory();
    private final SocketAddress address;

    private SocketChannel channel;

    public UnixDomainSocketConnection(SocketAddress address) {
        requireNonNull(address);

        this.address = address;
    }

    public static TransportConnection.Factory factory() {
        return factory;
    }

    @Override
    public TransportConnection connect() throws IOException {
        if (this.channel != null && this.channel.isConnected()) {
            return this;
        }

        this.channel = SocketChannel.open(this.address);
        return this;
    }

    @Override
    public TransportConnection disconnect() throws IOException {
        if (this.channel == null) {
            return this;
        }

        this.channel.close();
        this.channel = null;
        return this;
    }

    @Override
    public TransportConnection sendRaw(ByteBuf buf) throws IOException {
        var heap = ByteBuffer.allocate(128);
        do {
            heap.flip();
            heap.limit(Math.min(buf.readableBytes(), heap.capacity()));

            buf.readBytes(heap);
            heap.flip();
            this.channel.write(heap);
        } while (buf.isReadable());

        return this;
    }

    @Override
    public ByteBuf receive(int length) throws IOException {
        var buffer = ByteBuffer.allocate(length);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) == -1) {
                throw new IOException("End of stream reached. Connection has died.");
            }
        }
        buffer.flip();

        return Unpooled.wrappedBuffer(buffer);
    }

    private static class Factory implements TransportConnection.Factory {

        @Override
        public TransportConnection create(SocketAddress address) {
            return new UnixDomainSocketConnection(address);
        }

        @Override
        public String toString() {
            return "Unix Domain Socket";
        }
    }
}
