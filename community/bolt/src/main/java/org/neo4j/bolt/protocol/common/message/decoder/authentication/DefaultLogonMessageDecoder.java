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
package org.neo4j.bolt.protocol.common.message.decoder.authentication;

import java.util.Collections;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.util.AuthenticationMetadataUtils;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogonMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;

public final class DefaultLogonMessageDecoder implements MessageDecoder<LogonMessage> {
    private static final DefaultLogonMessageDecoder INSTANCE = new DefaultLogonMessageDecoder();

    private DefaultLogonMessageDecoder() {}

    public static DefaultLogonMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return LogonMessage.SIGNATURE;
    }

    @Override
    public LogonMessage read(Connection connection, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 1);

        var valueReader = connection.valueReader(buffer);
        var meta = AuthenticationMetadataUtils.convertExtraMap(
                valueReader, buffer.getTarget().readableBytes());
        var authToken = AuthenticationMetadataUtils.extractAuthToken(Collections.emptyList(), meta);

        return new LogonMessage(authToken);
    }
}
