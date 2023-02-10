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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.BeginMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.CommitMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.DiscardMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.HelloMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.PullMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.ResetMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RollbackMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.values.virtual.MapValue;

public abstract class AbstractBoltMessages implements BoltMessages {

    protected String getUserAgent() {
        return this.getClass().getSimpleName() + "/0.0";
    }

    protected Map<String, Object> getDefaultHelloMetaMap(Map<String, Object> overrides) {
        var result = new HashMap<>(overrides);

        if (!result.containsKey("user_agent")) {
            result.put("user_agent", this.getUserAgent());
        }

        return result;
    }

    @Override
    public RequestMessage hello(Map<String, Object> meta) {
        return new HelloMessage(this.getDefaultHelloMetaMap(meta));
    }

    @Override
    public RequestMessage reset() {
        return ResetMessage.INSTANCE;
    }

    @Override
    public RequestMessage goodbye() {
        return GoodbyeMessage.INSTANCE;
    }

    @Override
    public RequestMessage commit() {
        return CommitMessage.INSTANCE;
    }

    @Override
    public RequestMessage rollback() {
        return RollbackMessage.INSTANCE;
    }

    @Override
    public RequestMessage begin(
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode mode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser) {
        if (bookmarks == null) {
            bookmarks = emptyList();
        }
        if (txMetadata == null) {
            txMetadata = emptyMap();
        }
        if (databaseName == null) {
            databaseName = "";
        }
        if (impersonatedUser != null) {
            throw new UnsupportedOperationException("Impersonation is not supported in this protocol version");
        }

        return new BeginMessage(MapValue.EMPTY, bookmarks, txTimeout, mode, txMetadata, databaseName);
    }

    @Override
    public RequestMessage discard(long n, long statementId) {
        return new DiscardMessage(MapValue.EMPTY, n, statementId);
    }

    @Override
    public RequestMessage pull(long n, long statementId) {
        return new PullMessage(MapValue.EMPTY, n, statementId);
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
                db);
    }
}
