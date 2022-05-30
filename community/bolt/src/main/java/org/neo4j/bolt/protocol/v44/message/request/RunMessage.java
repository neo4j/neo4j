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
package org.neo4j.bolt.protocol.v44.message.request;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.values.virtual.MapValue;

public class RunMessage extends org.neo4j.bolt.protocol.v40.messaging.request.RunMessage {
    private final String impersonatedUser;

    public RunMessage(String statement) {
        super(statement);
        this.impersonatedUser = null;
    }

    public RunMessage(String statement, MapValue params) {
        super(statement, params);
        this.impersonatedUser = null;
    }

    public RunMessage(String statement, MapValue params, MapValue meta) {
        super(statement, params, meta);
        this.impersonatedUser = null;
    }

    public RunMessage(
            String statement,
            MapValue params,
            MapValue meta,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser) {
        super(statement, params, meta, bookmarks, txTimeout, accessMode, txMetadata, databaseName);
        this.impersonatedUser = impersonatedUser;
    }

    public String impersonatedUser() {
        return impersonatedUser;
    }
}
