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

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.List;
import java.util.Optional;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.bookmark.BookmarksParser;
import org.neo4j.bolt.protocol.v44.message.request.RouteMessage;
import org.neo4j.bolt.protocol.v44.message.util.MessageMetadataParserV44;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.value.PackstreamValues;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

public class RouteMessageDecoder implements StructReader<RouteMessage> {
    public static final String DB_KEY = "db";

    private final BookmarksParser bookmarksParser;

    public RouteMessageDecoder(BookmarksParser bookmarksParser) {
        this.bookmarksParser = bookmarksParser;
    }

    @Override
    public short getTag() {
        return RouteMessage.SIGNATURE;
    }

    @Override
    public RouteMessage read(PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        var routingContext = PackstreamValues.readMap(buffer);

        List<Bookmark> bookmarkList = List.of();
        if (buffer.peekType() != Type.NONE) {
            bookmarkList = bookmarksParser.parseBookmarks(PackstreamValues.readList(buffer));
        }

        var meta = PackstreamValues.readMap(buffer);

        var databaseName = Optional.of(meta.get(DB_KEY))
                .filter(any -> any != NO_VALUE && any instanceof TextValue)
                .map(any -> ((TextValue) any).stringValue())
                .orElse(null);

        var impersonatedUser = MessageMetadataParserV44.parseImpersonatedUser(meta);

        return this.newRouteMessage(routingContext, bookmarkList, meta, databaseName, impersonatedUser);
    }

    protected RouteMessage newRouteMessage(
            MapValue routingContext,
            List<Bookmark> bookmarkList,
            MapValue meta,
            String databaseName,
            String impersonatedUser) {
        return new RouteMessage(routingContext, bookmarkList, databaseName, impersonatedUser);
    }
}
