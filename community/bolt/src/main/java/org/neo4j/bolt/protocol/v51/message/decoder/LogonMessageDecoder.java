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

package org.neo4j.bolt.protocol.v51.message.decoder;

import java.util.Map;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.ReadMetadataUtils;
import org.neo4j.bolt.protocol.v51.message.request.LogonMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;

public class LogonMessageDecoder implements StructReader<Connection, LogonMessage> {
    private static final LogonMessageDecoder INSTANCE = new LogonMessageDecoder();

    public static LogonMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return LogonMessage.SIGNATURE;
    }

    @Override
    public LogonMessage read(Connection connection, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        if (header.length() != 1) {
            throw new IllegalStructSizeException(1, header.length());
        }

        var valueReader = connection.valueReader(buffer);

        // The only metadata on this tag is the auth token.
        Map<String, Object> authToken =
                this.readMetadataMap(valueReader, buffer.getTarget().readableBytes());

        return new LogonMessage(authToken);
    }

    protected Map<String, Object> readMetadataMap(PackstreamValueReader<Connection> reader, int limit)
            throws PackstreamReaderException {
        return ReadMetadataUtils.readMetadataMap(reader, limit);
    }
}
