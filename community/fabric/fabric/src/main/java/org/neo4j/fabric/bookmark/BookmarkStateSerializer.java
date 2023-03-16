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
package org.neo4j.fabric.bookmark;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;

import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;

public class BookmarkStateSerializer {
    public static String serialize(FabricBookmark fabricBookmark) {
        try {
            var packer = new Packer(fabricBookmark);
            var p = packer.pack();
            return p;
        } catch (IOException exception) {
            // if this fails, it means a bug
            throw new IllegalStateException("Failed to serialize bookmark", exception);
        }
    }

    public static FabricBookmark deserialize(String serializedBookmark) {
        try {
            var unpacker = new Unpacker(serializedBookmark);
            return unpacker.unpack();
        } catch (IOException exception) {
            throw new FabricException(InvalidBookmark, "Failed to deserialize bookmark", exception);
        }
    }

    private static class Packer {
        final PackstreamBuf buf = PackstreamBuf.allocUnpooled();
        final FabricBookmark fabricBookmark;

        Packer(FabricBookmark fabricBookmark) {
            this.fabricBookmark = fabricBookmark;
        }

        String pack() throws IOException {
            packInternalGraphs(fabricBookmark.getInternalGraphStates());
            packExternalGraphs(fabricBookmark.getExternalGraphStates());

            var heap = new byte[this.buf.getTarget().readableBytes()];
            buf.getTarget().readBytes(heap);

            return Base64.getEncoder().encodeToString(heap);
        }

        void packInternalGraphs(List<FabricBookmark.InternalGraphState> internalGraphStates) {
            buf.writeList(internalGraphStates, this::packInternalGraph);
        }

        void packInternalGraph(PackstreamBuf buf, FabricBookmark.InternalGraphState internalGraphState) {
            packUuid(buf, internalGraphState.getGraphUuid());
            buf.writeInt(internalGraphState.getTransactionId());
        }

        void packExternalGraphs(List<FabricBookmark.ExternalGraphState> externalGraphStates) {
            buf.writeList(externalGraphStates, this::packExternalGraph);
        }

        void packExternalGraph(PackstreamBuf buf, FabricBookmark.ExternalGraphState externalGraphState) {
            packUuid(buf, externalGraphState.getGraphUuid());
            buf.writeList(
                    externalGraphState.getBookmarks(), (b, bookmark) -> b.writeString(bookmark.getSerialisedState()));
        }

        void packUuid(PackstreamBuf buf, UUID uuid) {
            buf.writeBytes(Unpooled.buffer(16)
                    .writeLong(uuid.getMostSignificantBits())
                    .writeLong(uuid.getLeastSignificantBits()));
        }
    }

    private static class Unpacker {
        final PackstreamBuf buf;

        Unpacker(String serializedBookmark) {
            var bytes = Base64.getDecoder().decode(serializedBookmark);
            buf = PackstreamBuf.wrap(Unpooled.wrappedBuffer(bytes));
        }

        FabricBookmark unpack() throws IOException {
            try {
                var internalGraphs = unpackInternalGraphs();
                var externalGraphs = unpackExternalGraphs();

                return new FabricBookmark(internalGraphs, externalGraphs);
            } catch (PackstreamReaderException ex) {
                throw new IOException(ex);
            }
        }

        List<FabricBookmark.InternalGraphState> unpackInternalGraphs() throws IOException {
            return buf.readList(this::unpackInternalGraph);
        }

        FabricBookmark.InternalGraphState unpackInternalGraph(PackstreamBuf buf) throws PackstreamReaderException {
            UUID graphUuid = unpackUuid(buf);
            long txId = buf.readInt();

            return new FabricBookmark.InternalGraphState(graphUuid, txId);
        }

        List<FabricBookmark.ExternalGraphState> unpackExternalGraphs() throws IOException {
            return buf.readList(this::unpackExternalGraph);
        }

        FabricBookmark.ExternalGraphState unpackExternalGraph(PackstreamBuf buf) throws PackstreamReaderException {
            UUID graphUuid = unpackUuid(buf);
            var remoteBookmarks = buf.readList(PackstreamBuf::readString).stream()
                    .map(RemoteBookmark::new)
                    .collect(Collectors.toList());

            return new FabricBookmark.ExternalGraphState(graphUuid, remoteBookmarks);
        }

        UUID unpackUuid(PackstreamBuf buf) throws PackstreamReaderException {
            var uuidBytes = buf.readBytes();
            long high = uuidBytes.readLong();
            long low = uuidBytes.readLong();

            return new UUID(high, low);
        }
    }
}
