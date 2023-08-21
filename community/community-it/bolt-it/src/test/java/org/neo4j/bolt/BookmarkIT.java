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
package org.neo4j.bolt;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.util.ServerUtil;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Evaluates whether Bolt correctly parses, returns and handles bookmarks.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class BookmarkIT {

    @Inject
    private Neo4jWithSocket server;

    @ProtocolTest
    void shouldReturnUpdatedBookmarkAfterAutoCommitTransaction(
            BoltWire wire, @Authenticated TransportConnection connection) throws IOException {

        // bookmark is expected to advance once the auto-commit transaction is committed
        var lastClosedTransactionId = ServerUtil.getLastClosedTransactionId(this.server);
        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        ServerUtil.getDatabaseId(this.server).databaseId().uuid(), lastClosedTransactionId + 1)),
                List.of()));

        connection.send(wire.run("CREATE ()"));
        connection.send(wire.pull());

        assertThat(connection).receivesSuccess().receivesSuccess(map -> Assertions.assertThat(map)
                .containsEntry("bookmark", expectedBookmark));
    }

    @ProtocolTest
    void shouldReturnUpdatedBookmarkAfterExplicitTransaction(
            BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        // bookmark is expected to advance once the auto-commit transaction is committed
        var lastClosedTransactionId = ServerUtil.getLastClosedTransactionId(this.server);
        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        ServerUtil.getDatabaseId(this.server).databaseId().uuid(), lastClosedTransactionId + 1)),
                List.of()));

        connection.send(wire.begin());
        assertThat(connection).receivesSuccess();

        connection.send(wire.run("CREATE ()")).send(wire.pull());

        assertThat(connection).receivesSuccess().receivesSuccess(meta -> Assertions.assertThat(meta)
                .doesNotContainKey("bookmark"));

        connection.send(wire.commit());
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("bookmark", expectedBookmark));
    }
}
