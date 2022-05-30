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
package org.neo4j.bolt.protocol.v40.messaging.request;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40;
import org.neo4j.values.virtual.MapValue;

public abstract class TransactionInitiatingMessage implements RequestMessage {
    private final MapValue meta;
    private final List<Bookmark> bookmarks;
    private final Duration txTimeout;
    private final AccessMode accessMode;
    private final Map<String, Object> txMetadata;
    private final String databaseName;

    public TransactionInitiatingMessage() {
        this(MapValue.EMPTY, List.of(), null, AccessMode.WRITE, Map.of(), MessageMetadataParserV40.ABSENT_DB_NAME);
    }

    public TransactionInitiatingMessage(
            MapValue meta,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName) {
        this.meta = meta;
        this.bookmarks = bookmarks;
        this.txTimeout = txTimeout;
        this.accessMode = accessMode;
        this.txMetadata = txMetadata;
        this.databaseName = databaseName;
    }

    public List<Bookmark> bookmarks() {
        return bookmarks;
    }

    public Duration transactionTimeout() {
        return txTimeout;
    }

    public AccessMode getAccessMode() {
        return accessMode;
    }

    public Map<String, Object> transactionMetadata() {
        return txMetadata;
    }

    public String databaseName() {
        return databaseName;
    }

    public MapValue meta() {
        return meta;
    }

    @Override
    public boolean safeToProcessInAnyState() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionInitiatingMessage that = (TransactionInitiatingMessage) o;
        return Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(meta);
    }
}
