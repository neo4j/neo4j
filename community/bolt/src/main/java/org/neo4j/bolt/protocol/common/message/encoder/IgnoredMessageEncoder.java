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
package org.neo4j.bolt.protocol.common.message.encoder;

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.response.IgnoredMessage;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructWriter;

public final class IgnoredMessageEncoder implements StructWriter<Connection, IgnoredMessage> {
    private static final IgnoredMessageEncoder INSTANCE = new IgnoredMessageEncoder();

    private IgnoredMessageEncoder() {}

    public static IgnoredMessageEncoder getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<IgnoredMessage> getType() {
        return IgnoredMessage.class;
    }

    @Override
    public short getTag(IgnoredMessage payload) {
        return IgnoredMessage.SIGNATURE;
    }

    @Override
    public long getLength(IgnoredMessage payload) {
        return 0;
    }

    @Override
    public void write(Connection ctx, PackstreamBuf buffer, IgnoredMessage payload) {
        // NOOP - IGNORED does not contain any fields
    }
}
