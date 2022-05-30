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
package org.neo4j.bolt.protocol.v44.message.decoder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.bookmark.BookmarksParser;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.v44.message.request.BeginMessage;
import org.neo4j.bolt.protocol.v44.message.util.MessageMetadataParserV44;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.values.virtual.MapValue;

public class BeginMessageDecoder extends org.neo4j.bolt.protocol.v40.messaging.decoder.BeginMessageDecoder {

    public BeginMessageDecoder(BookmarksParser bookmarksParser) {
        super(bookmarksParser);
    }

    @Override
    protected BeginMessage newBeginMessage(
            MapValue metadata,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName)
            throws PackstreamReaderException {
        var impersonatedUser = MessageMetadataParserV44.parseImpersonatedUser(metadata);
        return newBeginMessage(metadata, bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser);
    }

    protected BeginMessage newBeginMessage(
            MapValue metadata,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser)
            throws PackstreamReaderException {
        return new BeginMessage(metadata, bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser);
    }
}
