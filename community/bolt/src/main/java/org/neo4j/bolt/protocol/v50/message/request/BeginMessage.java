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
package org.neo4j.bolt.protocol.v50.message.request;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.values.virtual.MapValue;

public class BeginMessage extends org.neo4j.bolt.protocol.v44.message.request.BeginMessage {
    private final TransactionType type;

    public BeginMessage(
            MapValue meta,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser,
            TransactionType type) {
        super(meta, bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser);
        this.type = type;
    }

    @Override
    public TransactionType type() {
        return type;
    }
}
