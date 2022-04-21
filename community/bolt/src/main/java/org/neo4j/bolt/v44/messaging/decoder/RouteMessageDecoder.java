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
package org.neo4j.bolt.v44.messaging.decoder;

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.bolt.v44.messaging.request.RouteMessage;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

public class RouteMessageDecoder implements RequestMessageDecoder {
    public static final String DB_KEY = "db";

    private final BoltResponseHandler responseHandler;
    private final BookmarksParser bookmarksParser;

    public RouteMessageDecoder(BoltResponseHandler responseHandler, BookmarksParser bookmarksParser) {
        this.responseHandler = responseHandler;
        this.bookmarksParser = bookmarksParser;
    }

    @Override
    public int signature() {
        return RouteMessage.SIGNATURE;
    }

    @Override
    public BoltResponseHandler responseHandler() {
        return this.responseHandler;
    }

    @Override
    public RequestMessage decode(Neo4jPack.Unpacker unpacker) throws IOException {
        var routingContext = unpacker.unpackMap();
        var bookmarkList = bookmarksParser.parseBookmarks(unpacker.unpack());

        var meta = unpacker.unpackMap();

        var databaseName = Optional.of(meta.get(DB_KEY))
                .filter(any -> any != NO_VALUE && any instanceof TextValue)
                .map(any -> ((TextValue) any).stringValue())
                .orElse(null);

        var impersonatedUser = MessageMetadataParser.parseImpersonatedUser(meta);

        return this.newRouteMessage(routingContext, bookmarkList, meta, databaseName, impersonatedUser);
    }

    protected RequestMessage newRouteMessage(
            MapValue routingContext,
            List<Bookmark> bookmarkList,
            MapValue meta,
            String databaseName,
            String impersonatedUser) {
        return new RouteMessage(routingContext, bookmarkList, databaseName, impersonatedUser);
    }
}
