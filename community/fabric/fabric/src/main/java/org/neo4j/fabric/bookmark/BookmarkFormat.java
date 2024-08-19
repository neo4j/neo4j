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
package org.neo4j.fabric.bookmark;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;

import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;

public class BookmarkFormat {

    // This prefix comes from the ancient times when this bookmark format was called "Fabric bookmark"
    // It had a status of an extension and the prefix was used to distinguish it from other formats.
    private static final String PREFIX = "FB:";

    public static List<QueryRouterBookmark> parse(List<String> customBookmarks) {
        return customBookmarks.stream().map(BookmarkFormat::parse).collect(Collectors.toList());
    }

    public static QueryRouterBookmark parse(String bookmarkString) {
        var content = bookmarkString.substring(PREFIX.length());

        if (content.isEmpty()) {
            return new QueryRouterBookmark(List.of(), List.of());
        }

        return BookmarkFormat.deserialize(content);
    }

    public static String serialize(QueryRouterBookmark queryRouterBookmark) {
        try {
            var packer = new Packer(queryRouterBookmark);
            var p = packer.pack();
            return PREFIX + p;
        } catch (IOException exception) {
            // if this fails, it means a bug
            throw new IllegalStateException("Failed to serialize bookmark", exception);
        }
    }

    private static QueryRouterBookmark deserialize(String serializedBookmark) {
        try {
            var unpacker = new Unpacker(serializedBookmark);
            return unpacker.unpack();
        } catch (Exception exception) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N12)
                    .withClassification(ErrorClassification.CLIENT_ERROR)
                    .build();
            throw new FabricException(
                    gql,
                    InvalidBookmark,
                    "Parsing of supplied bookmarks failed with message: " + exception.getMessage(),
                    exception);
        }
    }

    private static class Packer {
        final PackstreamBuf buf = PackstreamBuf.allocUnpooled();
        final QueryRouterBookmark queryRouterBookmark;

        Packer(QueryRouterBookmark queryRouterBookmark) {
            this.queryRouterBookmark = queryRouterBookmark;
        }

        String pack() throws IOException {
            packInternalGraphs(queryRouterBookmark.internalGraphStates());
            packExternalGraphs(queryRouterBookmark.externalGraphStates());

            var heap = new byte[this.buf.getTarget().readableBytes()];
            buf.getTarget().readBytes(heap);

            return Base64.getEncoder().encodeToString(heap);
        }

        void packInternalGraphs(List<QueryRouterBookmark.InternalGraphState> internalGraphStates) {
            buf.writeList(internalGraphStates, this::packInternalGraph);
        }

        void packInternalGraph(PackstreamBuf buf, QueryRouterBookmark.InternalGraphState internalGraphState) {
            packUuid(buf, internalGraphState.graphUuid());
            buf.writeInt(internalGraphState.transactionId());
        }

        void packExternalGraphs(List<QueryRouterBookmark.ExternalGraphState> externalGraphStates) {
            buf.writeList(externalGraphStates, this::packExternalGraph);
        }

        void packExternalGraph(PackstreamBuf buf, QueryRouterBookmark.ExternalGraphState externalGraphState) {
            packUuid(buf, externalGraphState.graphUuid());
            buf.writeList(externalGraphState.bookmarks(), (b, bookmark) -> b.writeString(bookmark.serializedState()));
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

        QueryRouterBookmark unpack() throws IOException {
            try {
                var internalGraphs = unpackInternalGraphs();
                var externalGraphs = unpackExternalGraphs();

                return new QueryRouterBookmark(internalGraphs, externalGraphs);
            } catch (PackstreamReaderException ex) {
                throw new IOException(ex);
            }
        }

        List<QueryRouterBookmark.InternalGraphState> unpackInternalGraphs() throws IOException {
            return buf.readList(this::unpackInternalGraph);
        }

        QueryRouterBookmark.InternalGraphState unpackInternalGraph(PackstreamBuf buf) throws PackstreamReaderException {
            UUID graphUuid = unpackUuid(buf);
            long txId = buf.readInt();

            return new QueryRouterBookmark.InternalGraphState(graphUuid, txId);
        }

        List<QueryRouterBookmark.ExternalGraphState> unpackExternalGraphs() throws IOException {
            return buf.readList(this::unpackExternalGraph);
        }

        QueryRouterBookmark.ExternalGraphState unpackExternalGraph(PackstreamBuf buf) throws PackstreamReaderException {
            UUID graphUuid = unpackUuid(buf);
            var remoteBookmarks = buf.readList(PackstreamBuf::readString).stream()
                    .map(RemoteBookmark::new)
                    .collect(Collectors.toList());

            return new QueryRouterBookmark.ExternalGraphState(graphUuid, remoteBookmarks);
        }

        UUID unpackUuid(PackstreamBuf buf) throws PackstreamReaderException {
            var uuidBytes = buf.readBytes();
            long high = uuidBytes.readLong();
            long low = uuidBytes.readLong();

            return new UUID(high, low);
        }
    }
}
