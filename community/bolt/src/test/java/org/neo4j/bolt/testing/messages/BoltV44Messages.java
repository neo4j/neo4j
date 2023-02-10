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
package org.neo4j.bolt.testing.messages;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.protocol.v44.message.request.BeginMessage;
import org.neo4j.bolt.protocol.v44.message.request.RunMessage;
import org.neo4j.values.virtual.MapValue;

public class BoltV44Messages extends BoltV43Messages {
    private static final BoltV44Messages INSTANCE = new BoltV44Messages();

    protected BoltV44Messages() {}

    public static BoltMessages getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return BoltProtocolV44.VERSION;
    }

    @Override
    public RequestMessage run(String statement, String db, MapValue params) {
        return new RunMessage(
                statement,
                params,
                MapValue.EMPTY,
                Collections.emptyList(),
                null,
                AccessMode.WRITE,
                Collections.emptyMap(),
                db,
                null);
    }

    @Override
    public RequestMessage begin(
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode mode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser) {
        return new BeginMessage(MapValue.EMPTY, bookmarks, txTimeout, mode, txMetadata, databaseName, impersonatedUser);
    }
}
