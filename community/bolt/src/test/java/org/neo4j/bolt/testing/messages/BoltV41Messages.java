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

import static org.neo4j.bolt.protocol.v40.messaging.request.RollbackMessage.INSTANCE;
import static org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40.ABSENT_DB_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.error.bookmark.BookmarkParserException;
import org.neo4j.bolt.protocol.v40.bookmark.BookmarkParserV40;
import org.neo4j.bolt.protocol.v40.messaging.request.BeginMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.DiscardMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.PullMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.ResetMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.bolt.protocol.v41.message.request.HelloMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

/**
 * Quick access of all Bolt V41 messages
 */
public class BoltV41Messages {
    private static final String USER_AGENT = "BoltV41Messages/0.0";
    private static final RequestMessage HELLO = new HelloMessage(
            map("user_agent", USER_AGENT),
            new RoutingContext(true, stringMap("policy", "fast", "region", "europe")),
            map("user_agent", USER_AGENT));
    private static final RequestMessage RUN_RETURN_ONE = new RunMessage("RETURN 1");
    private static final RequestMessage BEGIN = new BeginMessage();

    public static Stream<RequestMessage> supported() throws BoltIOException {
        return Stream.of(hello(), goodbye(), run(), discard(10), pull(10), begin(), commit(), rollback(), reset());
    }

    public static RequestMessage begin() throws BoltIOException {
        return BEGIN;
    }

    public static RequestMessage begin(DatabaseIdRepository repository, ListValue bookmarks)
            throws BookmarkParserException {
        var bookmarkList =
                new BookmarkParserV40(repository, CustomBookmarkFormatParser.DEFAULT).parseBookmarks(bookmarks);
        return new BeginMessage(MapValue.EMPTY, bookmarkList, null, AccessMode.WRITE, Map.of(), ABSENT_DB_NAME);
    }

    public static RequestMessage discard(long n) {
        return new DiscardMessage(MapValue.EMPTY, n, -1);
    }

    public static RequestMessage pull(long n) {
        return new PullMessage(MapValue.EMPTY, n, -1);
    }

    public static RequestMessage hello() {
        return HELLO;
    }

    public static RequestMessage hello(Map<String, Object> meta, RoutingContext routingContext) {
        if (!meta.containsKey("user_agent")) {
            meta.put("user_agent", USER_AGENT);
        }
        return new HelloMessage(meta, routingContext, meta);
    }

    public static RequestMessage run(String statement) {
        return new RunMessage(statement);
    }

    public static RequestMessage run() {
        return RUN_RETURN_ONE;
    }

    public static RequestMessage pull() throws BoltIOException {
        return pull(-1);
    }

    public static RequestMessage discard() throws BoltIOException {
        return discard(-1);
    }

    public static RequestMessage run(String statement, MapValue params) {
        return new RunMessage(statement, params);
    }

    public static RequestMessage run(String statement, MapValue params, MapValue meta) {
        return new RunMessage(statement, params, meta);
    }

    public static RequestMessage rollback() {
        return INSTANCE;
    }

    public static RequestMessage commit() {
        return INSTANCE;
    }

    public static RequestMessage reset() {
        return ResetMessage.INSTANCE;
    }

    public static RequestMessage goodbye() {
        return GoodbyeMessage.INSTANCE;
    }
}
