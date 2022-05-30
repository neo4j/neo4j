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
package org.neo4j.bolt.protocol.v40.messaging.decoder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.bookmark.BookmarksParser;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.v40.messaging.request.BeginMessage;
import org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValues;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.virtual.MapValue;

public class BeginMessageDecoder implements StructReader<BeginMessage> {
    private final BookmarksParser bookmarksParser; // TODO: Remove?

    public BeginMessageDecoder(BookmarksParser bookmarksParser) {
        this.bookmarksParser = bookmarksParser;
    }

    @Override
    public short getTag() {
        return BeginMessage.SIGNATURE;
    }

    @Override
    public BeginMessage read(PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        if (header.length() != 1) {
            throw new IllegalStructSizeException(1, header.length());
        }

        MapValue metadata;
        try {
            metadata = PackstreamValues.readMap(buffer);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("metadata", ex);
        }

        var bookmarks = MessageMetadataParserV40.parseBookmarks(bookmarksParser, metadata);
        try {
            var txTimeout = MessageMetadataParserV40.parseTransactionTimeout(metadata);
            var accessMode = MessageMetadataParserV40.parseAccessMode(metadata);
            var txMetadata = MessageMetadataParserV40.parseTransactionMetadata(metadata);
            var databaseName = MessageMetadataParserV40.parseDatabaseName(metadata);

            return newBeginMessage(metadata, bookmarks, txTimeout, accessMode, txMetadata, databaseName);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("metadata", ex);
        }
    }

    protected BeginMessage newBeginMessage(
            MapValue metadata,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName)
            throws PackstreamReaderException {
        return new BeginMessage(
                metadata, bookmarks, txTimeout, accessMode, txMetadata, databaseName); // v4 Begin Message
    }
}
