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

package org.neo4j.bolt.protocol.common.message.decoder.generic;

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.request.generic.TelemetryMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;

public class TelemetryMessageDecoder implements MessageDecoder<TelemetryMessage> {

    private static final TelemetryMessageDecoder INSTANCE = new TelemetryMessageDecoder();

    public static TelemetryMessageDecoder getInstance() {
        return INSTANCE;
    }

    private static final String apiKey = "api";

    @Override
    public short getTag() {
        return TelemetryMessage.SIGNATURE;
    }

    @Override
    public TelemetryMessage read(Connection connection, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 1);

        var valueReader = connection.valueReader(buffer);
        var apiType = valueReader.readLong();

        if (apiType != null) {
            var driverInterfaceTypeUsage = TelemetryMessage.DriverInterfaceType.fromLong(apiType.value());
            return new TelemetryMessage(driverInterfaceTypeUsage);
        } else {
            throw new IllegalStructArgumentException(apiKey, "Expected Integer but nothing was sent with the message");
        }
    }
}
