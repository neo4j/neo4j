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
package org.neo4j.bolt.testing.client;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.memory.ByteBuffers;

public class UnixDomainSocketConnection implements TransportConnection {

    private SocketChannel socketChannel;

    @Override
    public TransportConnection connect(HostnamePort address) throws Exception {
        throw new UnsupportedOperationException("UnixDomainSocketConnection can't connect to " + address);
    }

    @Override
    public TransportConnection connect(SocketAddress address) throws Exception {
        socketChannel = SocketChannel.open(address);
        return this;
    }

    @Override
    public TransportConnection send(byte[] rawBytes) throws IOException {
        socketChannel.write(ByteBuffer.wrap(rawBytes));
        return this;
    }

    @Override
    public byte[] recv(int length) throws IOException {
        ByteBuffer byteBuffer = ByteBuffers.allocate(length, ByteOrder.BIG_ENDIAN, INSTANCE);
        while (byteBuffer.hasRemaining()) {
            socketChannel.read(byteBuffer);
        }
        byteBuffer.flip();
        return byteBuffer.array();
    }

    @Override
    public void disconnect() throws IOException {
        if (socketChannel != null) {
            socketChannel.close();
        }
    }
}
