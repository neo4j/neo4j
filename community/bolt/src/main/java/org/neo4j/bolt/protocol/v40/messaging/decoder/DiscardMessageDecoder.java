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

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.v40.messaging.request.DiscardMessage;
import org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.virtual.MapValue;

public final class DiscardMessageDecoder implements StructReader<Connection, DiscardMessage> {
    private static final DiscardMessageDecoder INSTANCE = new DiscardMessageDecoder();

    private DiscardMessageDecoder() {}

    public static DiscardMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return DiscardMessage.SIGNATURE;
    }

    @Override
    public DiscardMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        if (header.length() != 1) {
            throw new IllegalStructSizeException(1, header.length());
        }

        var valueReader = ctx.valueReader(buffer);

        MapValue meta;
        try {
            meta = valueReader.readMap();
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("meta", ex);
        }

        try {
            var n = MessageMetadataParserV40.parseStreamLimit(meta);
            var statementId = MessageMetadataParserV40.parseStatementId(meta);

            return new DiscardMessage(meta, n, statementId);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("meta", ex);
        }
    }
}
