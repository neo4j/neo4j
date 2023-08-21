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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import org.neo4j.bolt.negotiation.ProtocolVersion;

public abstract class AbstractTransportConnection implements TransportConnection {
    private long noopCount;

    @Override
    public TransportConnection send(ProtocolVersion version) throws IOException {
        return this.send(version, ProtocolVersion.INVALID, ProtocolVersion.INVALID, ProtocolVersion.INVALID);
    }

    @Override
    public TransportConnection send(ProtocolVersion version1, ProtocolVersion version2) throws IOException {
        return this.send(version1, version2, ProtocolVersion.INVALID, ProtocolVersion.INVALID);
    }

    @Override
    public TransportConnection send(ProtocolVersion version1, ProtocolVersion version2, ProtocolVersion version3)
            throws IOException {
        return this.send(version1, version2, version3, ProtocolVersion.INVALID);
    }

    @Override
    public TransportConnection send(
            ProtocolVersion version1, ProtocolVersion version2, ProtocolVersion version3, ProtocolVersion version4)
            throws IOException {
        return this.sendRaw(Unpooled.buffer()
                .writeInt(0x6060B017)
                .writeInt(version1.encode())
                .writeInt(version2.encode())
                .writeInt(version3.encode())
                .writeInt(version4.encode()));
    }

    @Override
    public TransportConnection send(ByteBuf buf) throws IOException {
        while (buf.isReadable()) {
            var chunkLength = Math.min(buf.readableBytes(), 65_535);

            this.sendRaw(Unpooled.buffer().writeShort(chunkLength));
            this.sendRaw(buf.readSlice(chunkLength));
        }

        this.sendRaw(Unpooled.buffer().writeShort(0x0000));
        return this;
    }

    @Override
    public long noopCount() {
        return this.noopCount;
    }

    @Override
    public ProtocolVersion receiveNegotiatedVersion() throws IOException, InterruptedException {
        return new ProtocolVersion(this.receive(4).readInt());
    }

    @Override
    public int receiveChunkHeader() throws IOException, InterruptedException {
        return this.receive(2).readUnsignedShort();
    }

    @Override
    public ByteBuf receiveMessage() throws IOException, InterruptedException {
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
}
