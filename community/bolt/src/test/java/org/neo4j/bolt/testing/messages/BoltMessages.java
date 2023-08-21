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
package org.neo4j.bolt.testing.messages;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogoffMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogonMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.GoodbyeMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.ResetMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.common.message.request.streaming.DiscardMessage;
import org.neo4j.bolt.protocol.common.message.request.streaming.PullMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.CommitMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RollbackMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
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

    String getUserAgent();

    default boolean supportsLogonMessage() {
        return false;
    }

    default RequestMessage authenticate(String principal, String credentials) {
        return this.logon(principal, credentials);
    }

    default RequestMessage hello() {
        return this.hello(Collections.emptyMap());
    }

    default RequestMessage hello(Map<String, Object> authToken) {
        return this.hello(Collections.emptyList(), null, authToken);
    }

    default RequestMessage hello(RoutingContext routingContext) {
        return this.hello(Collections.emptyList(), routingContext, Collections.emptyMap());
    }

    RequestMessage hello(List<Feature> features, RoutingContext routingContext, Map<String, Object> authToken);

    default RequestMessage hello(String principal, String credentials) {
        return this.hello(AuthToken.newBasicAuthToken(principal, credentials));
    }

    default RequestMessage logon() {
        return new LogonMessage(Collections.emptyMap());
    }

    default RequestMessage logon(String principal, String credentials) {
        return new LogonMessage(AuthToken.newBasicAuthToken(principal, credentials));
    }

    default RequestMessage logoff() {
        return LogoffMessage.getInstance();
    }

    default RequestMessage reset() {
        return ResetMessage.getInstance();
    }

    default RequestMessage goodbye() {
        return GoodbyeMessage.getInstance();
    }

    default RequestMessage route() {
        return new RouteMessage(MapValue.EMPTY, Collections.emptyList(), null, null);
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
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode mode,
            Map<String, Object> txMetadata,
            String databaseName) {
        return this.begin(bookmarks, txTimeout, mode, txMetadata, databaseName, null);
    }

    default RequestMessage begin(
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode mode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser) {
        return new BeginMessage(bookmarks, null, mode, txMetadata, databaseName, impersonatedUser);
    }

    default RequestMessage commit() {
        return CommitMessage.getInstance();
    }

    default RequestMessage rollback() {
        return RollbackMessage.getInstance();
    }

    default RequestMessage discard(long n, long statementId) {
        return new DiscardMessage(n, statementId);
    }

    default RequestMessage discard(long n) {
        return discard(n, -1);
    }

    default RequestMessage discard() {
        return discard(-1);
    }

    default RequestMessage pull(long n, long statementId) {
        return new PullMessage(n, statementId);
    }

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

    default RequestMessage run(String statement, String db, MapValue params) {
        return new RunMessage(
                statement,
                params,
                Collections.emptyList(),
                null,
                AccessMode.WRITE,
                Collections.emptyMap(),
                db,
                null,
                null);
    }
}
