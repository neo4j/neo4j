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
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

public interface BoltMessages {

    String RETURN_ONE_QUERY = "RETURN 1";

    /**
     * Retrieves the protocol version which is provided by this implementation.
     *
     * @return a protocol version.
     */
    ProtocolVersion version();

    default RequestMessage hello(RoutingContext routingContext) {
        throw new UnsupportedOperationException("Routing is not supported by this protocol version");
    }

    @Deprecated // TODO: This sucks
    RequestMessage hello(Map<String, Object> meta);

    default RequestMessage hello() {
        return this.hello(Collections.emptyMap());
    }

    default RequestMessage hello(String principal, String credentials) {
        return this.hello(AuthToken.newBasicAuthToken(principal, credentials));
    }

    RequestMessage reset();

    RequestMessage goodbye();

    default RequestMessage route() {
        throw new UnsupportedOperationException("ROUTE is not supported by this protocol version");
    }

    default RequestMessage begin() {
        return this.begin(Collections.emptyList(), null, AccessMode.WRITE, Collections.emptyMap(), null);
    }

    default RequestMessage begin(String databaseName) {
        return this.begin(Collections.emptyList(), null, AccessMode.WRITE, Collections.emptyMap(), databaseName);
    }

    default RequestMessage begin(String databaseName, String impersonatedUser) {
        return this.begin(
                Collections.emptyList(),
                null,
                AccessMode.WRITE,
                Collections.emptyMap(),
                databaseName,
                impersonatedUser);
    }

    default RequestMessage begin(
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode mode,
            Map<String, Object> txMetadata,
            String databaseName) {
        return this.begin(bookmarks, txTimeout, mode, txMetadata, databaseName, null);
    }

    RequestMessage begin(
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode mode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser);

    RequestMessage commit();

    RequestMessage rollback();

    RequestMessage discard(long n, long statementId);

    default RequestMessage discard(long n) {
        return discard(n, -1);
    }

    default RequestMessage discard() {
        return discard(-1);
    }

    RequestMessage pull(long n, long statementId);

    default RequestMessage pull(long n) {
        return this.pull(n, -1);
    }

    default RequestMessage pull() {
        return this.pull(-1);
    }

    default RequestMessage run() {
        return this.run(RETURN_ONE_QUERY);
    }

    default RequestMessage run(String statement) {
        return this.run(statement, VirtualValues.EMPTY_MAP);
    }

    default RequestMessage run(String statement, String db) {
        return this.run(statement, db, MapValue.EMPTY);
    }

    default RequestMessage run(String statement, MapValue params) {
        return this.run(statement, "", params);
    }

    RequestMessage run(String statement, String db, MapValue params);

    default RequestMessage logon() {
        throw new UnsupportedOperationException("LOGON is not supported by this protocol version");
    }

    default RequestMessage logoff() {
        throw new UnsupportedOperationException("LOGOFF is not supported by this protocol version");
    }
}
