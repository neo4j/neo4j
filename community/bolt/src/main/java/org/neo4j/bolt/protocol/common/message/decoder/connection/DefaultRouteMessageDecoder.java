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

import java.util.Collections;
import java.util.List;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.transaction.AbstractTransactionInitiatingMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.util.TransactionInitiatingMetadataParser;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;

public final class DefaultRouteMessageDecoder implements MessageDecoder<RouteMessage> {
    private static final DefaultRouteMessageDecoder INSTANCE = new DefaultRouteMessageDecoder();

    private DefaultRouteMessageDecoder() {}

    public static DefaultRouteMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return RouteMessage.SIGNATURE;
    }

    @Override
    public RouteMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 3);

        var valueReader = ctx.valueReader(buffer);
        var routingContext = valueReader.readMap();

        var bookmarkList = this.readBookmarks(buffer, valueReader);
        var meta = valueReader.readMap();

        var databaseName = TransactionInitiatingMetadataParser.readDatabaseName(meta);
        var impersonatedUser = TransactionInitiatingMetadataParser.readImpersonatedUser(meta);

        return new RouteMessage(routingContext, bookmarkList, databaseName, impersonatedUser);
    }

    protected List<String> readBookmarks(PackstreamBuf buffer, PackstreamValueReader<Connection> valueReader)
            throws PackstreamReaderException {
        if (buffer.peekType() == Type.NONE) {
            return Collections.emptyList();
        }

        return AbstractTransactionInitiatingMessageDecoder.convertBookmarks(valueReader.readList());
    }
}
