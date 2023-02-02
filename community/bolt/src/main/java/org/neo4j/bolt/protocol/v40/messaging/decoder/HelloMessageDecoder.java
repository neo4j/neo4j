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

import java.util.Map;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.ReadMetadataUtils;
import org.neo4j.bolt.protocol.v40.messaging.request.HelloMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;

public class HelloMessageDecoder implements StructReader<Connection, HelloMessage> {
    private static final HelloMessageDecoder INSTANCE = new HelloMessageDecoder();

    protected HelloMessageDecoder() {}

    public static HelloMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return HelloMessage.SIGNATURE;
    }

    @Override
    public HelloMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        if (header.length() != 1) {
            throw new IllegalStructSizeException(1, header.length());
        }

        var reader = ctx.valueReader(buffer);

        var meta = this.readMetaDataMap(reader, buffer.getTarget().readableBytes());
        this.validateMeta(meta);

        return new HelloMessage(meta);
    }

    protected void validateMeta(Map<String, Object> meta) throws PackstreamReaderException {
        var userAgent = meta.get("user_agent");
        if (userAgent == null) {
            throw new IllegalStructArgumentException("user_agent", "Expected \"user_agent\" to be non-null");
        }
        if (!(userAgent instanceof String)) {
            throw new IllegalStructArgumentException("user_agent", "Expected value to be a string");
        }
    }

    protected Map<String, Object> readMetaDataMap(PackstreamValueReader<Connection> reader, int limit)
            throws PackstreamReaderException {
        return ReadMetadataUtils.readMetadataMap(reader, limit);
    }
}
