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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.util.TransactionInitiatingMetadataParser;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;
import org.neo4j.values.virtual.MapValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;

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
            throw IllegalStructArgumentException.protocolError("statement", ex);
        }
        try {
            params = valueReader.readMap();
        } catch (PackstreamReaderException ex) {
            throw IllegalStructArgumentException.protocolError("params", ex);
        }
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

            var compressedStatement = this.readCompressedStatement(metadata);
            if (compressedStatement != null && !(statement.isEmpty())) {
                throw new IllegalArgumentException("metadata.compressedStatement and statement cannot both be present");
            }
            else if (compressedStatement != null) {
                try {
                    Inflater inflater = new Inflater();
                    inflater.setInput(compressedStatement);

                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    byte[] decompressed = new byte[]{};

                    while (!inflater.finished()) {
                        int decompressedSize = inflater.inflate(decompressed);
                        outputStream.write(decompressed, 0, decompressedSize);
                    }
                    statement = outputStream.toString(StandardCharsets.UTF_8);
                } catch (DataFormatException ex) {
                    throw new IllegalArgumentException("metadata.compressedStatement cannot be decompressed");
                }
            }

            String paramsAsJson;
            var compressedParams = this.readCompressedParams(metadata);
            if (compressedParams != null && !(params.isEmpty())) {
                throw new IllegalArgumentException("metadata.compressedParams and params cannot both be present");
            }
            else if (compressedParams != null) {
                try {
                    Inflater inflater = new Inflater();
                    inflater.setInput(compressedStatement);

                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    byte[] decompressed = new byte[]{};

                    while (!inflater.finished()) {
                        int decompressedSize = inflater.inflate(decompressed);
                        outputStream.write(decompressed, 0, decompressedSize);
                    }
                    paramsAsJson = outputStream.toString(StandardCharsets.UTF_8);
                } catch (DataFormatException ex) {
                    throw new IllegalArgumentException("metadata.compressedParams cannot be decompressed");
                }

                try {
                    Map<String,Object> paramsMap = new ObjectMapper().readValue(paramsAsJson, Map.class);
                } catch (JsonProcessingException ex) {
                    throw new IllegalArgumentException("metadata.compressedParams was uncompressed but cannot be parsed into a valid map");
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
        } catch (PackstreamReaderException ex) {
            throw IllegalStructArgumentException.protocolError("metadata", ex);
        }
    }
}
