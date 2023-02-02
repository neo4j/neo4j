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
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.protocol.v44.message.request.RouteMessage;
import org.neo4j.values.virtual.MapValue;

public final class BoltDefaultMessages {
    private BoltDefaultMessages() {}

    public static RequestMessage hello() {
        return BoltV51Messages.hello();
    }

    public static RequestMessage hello(Map<String, Object> meta) {
        return BoltV51Messages.hello(meta);
    }

    public static RequestMessage hello(Map<String, Object> meta, RoutingContext routingContext) {
        return BoltV51Messages.hello(meta, routingContext);
    }

    public static RequestMessage run() {
        return BoltV51Messages.run();
    }

    public static RequestMessage run(String statement) {
        return BoltV51Messages.run(statement);
    }

    public static RequestMessage run(String statement, MapValue params) {
        return BoltV51Messages.run(statement, params);
    }

    public static RequestMessage run(String statement, MapValue params, MapValue meta) {
        return BoltV51Messages.run(statement, params, meta);
    }

    public static RequestMessage pull() {
        return BoltV51Messages.pull();
    }

    public static RequestMessage discard() throws BoltIOException {
        return BoltV51Messages.discard();
    }

    public static RequestMessage begin() {
        return BoltV51Messages.begin();
    }

    public static RequestMessage begin(String databaseName) {
        return BoltV51Messages.begin(databaseName);
    }

    public static RequestMessage begin(String databaseName, String impersonatedUser) {
        return BoltV51Messages.begin(databaseName, impersonatedUser);
    }

    public static RequestMessage begin(
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser) {
        return BoltV44Messages.begin(bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser);
    }

    public static RequestMessage rollback() {
        return BoltV51Messages.rollback();
    }

    public static RequestMessage commit() {
        return BoltV51Messages.commit();
    }

    public static RequestMessage reset() {
        return BoltV51Messages.reset();
    }

    public static RequestMessage goodbye() {
        return BoltV51Messages.goodbye();
    }

    public static RouteMessage route(String impersonatedUser) {
        return BoltV51Messages.route(impersonatedUser);
    }
}
