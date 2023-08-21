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
package org.neo4j.bolt.protocol.common.message.decoder.transaction;

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.util.TransactionInitiatingMetadataParser;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;
import org.neo4j.values.virtual.MapValue;

public class DefaultRunMessageDecoder extends AbstractTransactionInitiatingMessageDecoder<RunMessage> {
    private static final DefaultRunMessageDecoder INSTANCE = new DefaultRunMessageDecoder();

    protected DefaultRunMessageDecoder() {}

    public static DefaultRunMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return RunMessage.SIGNATURE;
    }

    @Override
    public RunMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 3);

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
            var bookmarks = this.readBookmarks(metadata);
            var txTimeout = this.readTimeout(metadata);
            var accessMode = this.readAccessMode(metadata);
            var txMetadata = this.readMetadata(metadata);
            var databaseName = TransactionInitiatingMetadataParser.readDatabaseName(metadata);
            var impersonatedUser = this.readImpersonatedUser(metadata);
            var notificationsConfig = this.readNotificationsConfig(metadata);
            return new RunMessage(
                    statement,
                    params,
                    bookmarks,
                    txTimeout,
                    accessMode,
                    txMetadata,
                    databaseName,
                    impersonatedUser,
                    notificationsConfig);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("metadata", ex);
        }
    }
}
