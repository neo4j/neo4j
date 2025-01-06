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

package org.neo4j.bolt.protocol.v52.message.decoder.transaction;

import org.neo4j.bolt.protocol.common.message.decoder.transaction.DefaultRunMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.util.NotificationsConfigMetadataReader;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.virtual.MapValue;

import io.netty.handler.codec.compression.JdkZlibDecoder;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public final class RunMessageDecoderV52 extends DefaultRunMessageDecoder {
    private static final RunMessageDecoderV52 INSTANCE = new RunMessageDecoderV52();

    private RunMessageDecoderV52() {}

    public static RunMessageDecoderV52 getInstance() {
        return INSTANCE;
    }

    @Override
    protected NotificationsConfig readNotificationsConfig(MapValue meta) throws IllegalStructArgumentException {
        return NotificationsConfigMetadataReader.readLegacyFromMapValue(meta);
    }

    @Override
    public RunMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 3);

        var valueReader = ctx.valueReader(buffer);

        String statement;
        MapValue params;
        MapValue metadata;
        Boolean compressed;

        // First the metadata, because we need to know if the payload is compressed, which determines the type of the statement and params

        try {
            metadata = valueReader.readMap();
        } catch (PackstreamReaderException ex) {
            throw IllegalStructArgumentException.protocolError("metadata", ex);
        }

        try {
            var bookmarks = this.readBookmarks(metadata);
            var txTimeout = this.readTimeout(metadata);
            var accessMode = this.readAccessMode(metadata);
            var txMetadata = this.readMetadata(metadata);
            var databaseName = TransactionInitiatingMetadataParser.readDatabaseName(metadata);
            var impersonatedUser = this.readImpersonatedUser(metadata);
            var notificationsConfig = this.readNotificationsConfig(metadata);
            compressed = this.readStatementCompressed(metadata);
        } catch (PackstreamReaderException ex) {
            throw IllegalStructArgumentException.protocolError("metadata", ex);
        }

        if (compressed) {
            try {

                statement = buffer.readBytes();
                byte[] bytes = new byte[statement.readableBytes()];
                var log = logProvider.getLog(getClass());
                log.debug(
                "777777777777 Run compressed '%s'",
                bytes.toString();

                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream);
                deflaterOutputStream.write(bytes);
                deflaterOutputStream.flush();
                deflaterOutputStream.close();
                statement = Unpooled.copiedBuffer(bytes);

            } catch (PackstreamReaderException ex) {
                throw IllegalStructArgumentException.protocolError("statement", ex);
            }

            try {
                params = valueReader.readBytes();
            } catch (PackstreamReaderException ex) {
                throw IllegalStructArgumentException.protocolError("params", ex);
            }

        } else {
            try {
                statement = buffer.readString();
            } catch (PackstreamReaderException ex) {
                throw IllegalStructArgumentException.protocolError("statement", ex);
            }

            try {
                params = valueReader.readMap();
            } catch (PackstreamReaderException ex) {
                throw IllegalStructArgumentException.protocolError("params", ex);
            }
        }

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
    }

}
