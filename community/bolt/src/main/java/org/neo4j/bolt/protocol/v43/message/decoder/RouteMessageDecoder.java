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
package org.neo4j.bolt.protocol.v43.message.decoder;

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.List;
import java.util.Optional;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.v43.message.request.RouteMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.TextValue;

/**
 * Responsible for decoding the RouteMessage and its fields.
 */
public class RouteMessageDecoder implements StructReader<Connection, RouteMessage> {
    private static final RouteMessageDecoder INSTANCE = new RouteMessageDecoder();

    protected RouteMessageDecoder() {}

    public static RouteMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return RouteMessage.SIGNATURE;
    }

    @Override
    public RouteMessage read(Connection connection, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        if (header.length() != 3) {
            throw new IllegalStructSizeException(3, header.length());
        }

        var valueReader = connection.valueReader(buffer);
        var routingContext = valueReader.readMap();

        List<Bookmark> bookmarkList = List.of();
        if (buffer.peekType() != Type.NONE) {
            bookmarkList = connection.connector().bookmarkParser().parseBookmarks(valueReader.readList());
        }

        var databaseName = Optional.of(valueReader.readValue())
                .filter(any -> any != NO_VALUE && any instanceof TextValue)
                .map(any -> ((TextValue) any).stringValue())
                .orElse(null);

        return new RouteMessage(routingContext, bookmarkList, databaseName);
    }
}
