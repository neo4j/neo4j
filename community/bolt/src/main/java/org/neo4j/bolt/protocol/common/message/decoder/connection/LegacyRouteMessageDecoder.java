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
package org.neo4j.bolt.protocol.common.message.decoder.connection;

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.List;
import java.util.Optional;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.transaction.AbstractTransactionInitiatingMessageDecoder;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;
import org.neo4j.values.storable.TextValue;

public final class LegacyRouteMessageDecoder implements MessageDecoder<RouteMessage> {
    private static final LegacyRouteMessageDecoder INSTANCE = new LegacyRouteMessageDecoder();

    private LegacyRouteMessageDecoder() {}

    public static LegacyRouteMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return RouteMessage.SIGNATURE;
    }

    @Override
    public RouteMessage read(Connection connection, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 3);

        var valueReader = connection.valueReader(buffer);
        var routingContext = valueReader.readMap();

        var bookmarkList = List.<String>of();
        if (buffer.peekType() != Type.NONE) {
            bookmarkList = AbstractTransactionInitiatingMessageDecoder.convertBookmarks(valueReader.readList());
        }

        var databaseName = Optional.of(valueReader.readValue())
                .filter(any -> any != NO_VALUE && any instanceof TextValue)
                .map(any -> ((TextValue) any).stringValue())
                .orElse(null);

        return new RouteMessage(routingContext, bookmarkList, databaseName, null);
    }
}
