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
package org.neo4j.bolt.protocol.io.reader;

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.value.AbstractPackstreamValueReader;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;

public class ConnectionPackstreamValueReader extends AbstractPackstreamValueReader {

    private final Connection connection;
    private final StructRegistry<Connection, Value> structRegistry;

    public ConnectionPackstreamValueReader(
            PackstreamBuf buf, Connection connection, StructRegistry<Connection, Value> structRegistry) {
        super(buf);
        this.connection = connection;
        this.structRegistry = structRegistry;
    }

    @Override
    protected AnyValue readPrimitiveValue(Type type, long limit) throws PackstreamReaderException {
        var protocol = this.connection.protocol();
        if (!protocol.supportsPackstreamType(type)) {
            throw UnexpectedTypeException.wrongType(
                    String.valueOf(limit),
                    protocol.supportedPackstreamTypes().stream()
                            .map(Type::toString)
                            .toList(),
                    type);
        }

        return super.readPrimitiveValue(type, limit);
    }

    @Override
    public Value readStruct() throws PackstreamReaderException {
        return this.buf.readStruct(this.connection, this.structRegistry);
    }
}
