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
import org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

public class RunMessage extends TransactionInitiatingMessage {
    public static final byte SIGNATURE = 0x10;

    private final String statement;
    private final MapValue params;

    public RunMessage(String statement) {
        this(statement, VirtualValues.EMPTY_MAP);
    }

    public RunMessage(String statement, MapValue params) {
        this(statement, params, VirtualValues.EMPTY_MAP);
    }

    public RunMessage(String statement, MapValue params, MapValue meta) {
        this(
                statement,
                params,
                meta,
                List.of(),
                null,
                AccessMode.WRITE,
                Map.of(),
                MessageMetadataParserV40.ABSENT_DB_NAME);
    }

    public RunMessage(
            String statement,
            MapValue params,
            MapValue meta,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName) {
        super(meta, bookmarks, txTimeout, accessMode, txMetadata, databaseName);
        this.statement = statement;
        this.params = params;
    }

    public String statement() {
        return statement;
    }

    public MapValue params() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        RunMessage that = (RunMessage) o;
        return Objects.equals(statement, that.statement) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), statement, params);
    }

    @Override
    public String toString() {
        return "RUN \"" + this.statement + "\", " + this.params + ", " + meta();
    }
}
