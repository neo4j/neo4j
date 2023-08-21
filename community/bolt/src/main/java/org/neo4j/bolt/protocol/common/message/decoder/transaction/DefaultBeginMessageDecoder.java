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

import java.util.Locale;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.util.TransactionInitiatingMetadataParser;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;
import org.neo4j.packstream.util.PackstreamConversions;
import org.neo4j.values.virtual.MapValue;

public class DefaultBeginMessageDecoder extends AbstractTransactionInitiatingMessageDecoder<BeginMessage> {
    private static final DefaultBeginMessageDecoder INSTANCE = new DefaultBeginMessageDecoder();

    protected DefaultBeginMessageDecoder() {}

    public static DefaultBeginMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return BeginMessage.SIGNATURE;
    }

    @Override
    public BeginMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 1);

        var valueReader = ctx.valueReader(buffer);

        MapValue metadata;
        try {
            metadata = valueReader.readMap();
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("metadata", ex);
        }

        var bookmarks = this.readBookmarks(metadata);
        try {
            var timeout = this.readTimeout(metadata);
            var accessMode = this.readAccessMode(metadata);
            var txMetadata = this.readMetadata(metadata);
            var databaseName = TransactionInitiatingMetadataParser.readDatabaseName(metadata);
            var impersonatedUser = this.readImpersonatedUser(metadata);
            var type = this.readType(metadata);
            var notificationsConfig = this.readNotificationsConfig(metadata);

            return new BeginMessage(
                    bookmarks,
                    timeout,
                    accessMode,
                    txMetadata,
                    databaseName,
                    impersonatedUser,
                    type,
                    notificationsConfig);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("metadata", ex);
        }
    }

    protected TransactionType readType(MapValue meta) throws PackstreamReaderException {
        var transactionType = PackstreamConversions.asNullableStringValue(FIELD_TYPE, meta.get(FIELD_TYPE));
        if (transactionType == null) {
            return TransactionType.EXPLICIT;
        }

        try {
            return TransactionType.valueOf(transactionType.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            // TODO: We currently ignore any invalid values - Is this really desirable?
            return TransactionType.EXPLICIT;
        }
    }
}
