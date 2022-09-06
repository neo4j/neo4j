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
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.virtual.MapValue;

public class RunMessageDecoder implements StructReader<Connection, RunMessage> {
    private static final RunMessageDecoder INSTANCE = new RunMessageDecoder();

    protected RunMessageDecoder() {}

    public static RunMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return RunMessage.SIGNATURE;
    }

    @Override
    public RunMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        if (header.length() != 3) {
            throw new IllegalStructSizeException(3, header.length());
        }

        var valueReader = ctx.valueReader(buffer);

        String statement;
        MapValue params;
        MapValue metadata;
        try {
            statement = buffer.readString();
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("statement", ex);
        }
        try {
            params = valueReader.readMap();
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("params", ex);
        }
        try {
            metadata = valueReader.readMap();
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("metadata", ex);
        }

        try {
            var bookmarks =
                    MessageMetadataParserV40.parseBookmarks(ctx.connector().bookmarkParser(), metadata);
            var txTimeout = MessageMetadataParserV40.parseTransactionTimeout(metadata);
            var accessMode = MessageMetadataParserV40.parseAccessMode(metadata);
            var txMetadata = MessageMetadataParserV40.parseTransactionMetadata(metadata);
            var databaseName = MessageMetadataParserV40.parseDatabaseName(metadata);

            return newRunMessage(
                    statement, params, metadata, bookmarks, txTimeout, accessMode, txMetadata, databaseName);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("metadata", ex);
        }
    }

    protected RunMessage newRunMessage(
            String statement,
            MapValue params,
            MapValue meta,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName)
            throws PackstreamReaderException {
        return new RunMessage(
                statement, params, meta, bookmarks, txTimeout, accessMode, txMetadata, databaseName); // v4 RUN message
    }
}
