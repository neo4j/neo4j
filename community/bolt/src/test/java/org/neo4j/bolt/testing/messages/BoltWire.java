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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.factory.BeginMessageBuilder;
import org.neo4j.bolt.testing.messages.factory.HelloMessageBuilder;
import org.neo4j.bolt.testing.messages.factory.RunMessageBuilder;
import org.neo4j.bolt.testing.messages.factory.TelemetryMessageBuilder;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.values.virtual.MapValue;

public interface BoltWire {

    static Stream<BoltWire> versions() {
        return Stream.of(
                new BoltV40Wire(),
                new BoltV41Wire(),
                new BoltV42Wire(),
                new BoltV43Wire(),
                new BoltV44Wire(),
                new BoltV50Wire(),
                new BoltV51Wire(),
                new BoltV52Wire(),
                new BoltV53Wire(),
                new BoltV54Wire(),
                new BoltV56Wire());
    }

    /**
     * Will be true if it uses the newer authentication method when auth is sent in a logon message not the hello message
     * @return whether the new auth is being used
     */
    default boolean supportsLogonMessage() {
        return true;
    }

    /**
     * Identifies the revision of the protocol which is targeted by this wire implementation.
     *
     * @return a protocol version.
     */
    ProtocolVersion getProtocolVersion();

    /**
     * Get a User agent string.
     * @return a user agent string.
     */
    String getUserAgent();

    WriterPipeline getPipeline();

    /**
     * Transmits a negotiation request for the protocol version implemented by this wire and ensures that the correct version is selected.
     *
     * @param connection a connection.
     * @throws IOException when transmitting the request fails.
     */
    default void negotiate(TransportConnection connection) throws IOException {
        connection.send(this.getProtocolVersion());

        BoltConnectionAssertions.assertThat(connection).negotiates(this.getProtocolVersion());
    }

    /**
     * Enables a given set of features within this bolt wire.
     *
     * @param features an array of features.
     */
    void enable(Feature... features);

    /**
     * Disables a given set of features within this bolt wire.
     *
     * @param features an array of features.
     */
    void disable(Feature... features);

    /**
     * Retrieves a listing of features which have been enabled on this connection.
     *
     * @return a list of features.
     */
    Set<Feature> getEnabledFeatures();

    /**
     * Checks whether the wire implementation recognizes the desired feature(s) as optional negotiated functionality.
     *
     * @param features an array of features.
     */
    boolean isOptionalFeature(Feature... features);

    default ByteBuf hello() {
        return hello(x -> x);
    }

    default ByteBuf hello(UnaryOperator<HelloMessageBuilder> fn) {
        return fn.apply(new HelloMessageBuilder(
                        this.getProtocolVersion(), this.getUserAgent(), this.getEnabledFeatures(), this.getBoltAgent()))
                .build();
    }

    default Map<String, String> getBoltAgent() {
        return Collections.emptyMap();
    }

    default ByteBuf logon() {
        return this.logon(new HashMap<>());
    }

    default ByteBuf logon(String principal, String credentials) {
        return this.logon(Map.of("scheme", "basic", "principal", principal, "credentials", credentials));
    }

    default ByteBuf logon(String principal, String credentials, String realm) {
        return this.logon(
                Map.of("scheme", "basic", "principal", principal, "credentials", credentials, "realm", realm));
    }

    ByteBuf logon(Map<String, Object> authToken);

    ByteBuf logoff();

    default ByteBuf begin() {
        return begin(x -> x);
    }

    default ByteBuf begin(UnaryOperator<BeginMessageBuilder> fn) {
        return fn.apply(new BeginMessageBuilder(this.getProtocolVersion())).build();
    }

    default ByteBuf discard() {
        return this.discard(-1);
    }

    ByteBuf discard(long n);

    default ByteBuf pull() {
        return this.pull(-1);
    }

    ByteBuf pull(long n);

    ByteBuf pull(long n, long qid);

    default ByteBuf run(String statement) {
        return this.run(statement, x -> x);
    }

    default ByteBuf run(String statement, MapValue params) {
        return this.run(statement, x -> x.withParameters(params));
    }

    default ByteBuf run(String statement, UnaryOperator<RunMessageBuilder> fn) {
        return fn.apply(new RunMessageBuilder(this.getProtocolVersion(), statement, this.getPipeline()))
                .build();
    }

    ByteBuf rollback();

    ByteBuf commit();

    ByteBuf reset();

    ByteBuf goodbye();

    default ByteBuf route() {
        return this.route(null);
    }

    default ByteBuf route(String impersonatedUser) {
        return this.route(null, null, null, impersonatedUser);
    }

    default ByteBuf route(RoutingContext context, Collection<String> bookmarks, String db) {
        return this.route(context, bookmarks, db, null);
    }

    ByteBuf route(RoutingContext context, Collection<String> bookmarks, String db, String impersonatedUser);

    default ByteBuf telemetry(UnaryOperator<TelemetryMessageBuilder> fn) {
        return fn.apply(new TelemetryMessageBuilder(this.getProtocolVersion())).build();
    }

    void nodeValue(PackstreamBuf buf, String elementId, int id, List<String> labels);

    void relationshipValue(
            PackstreamBuf buf,
            String elementId,
            int id,
            String startElementId,
            int startId,
            String endElementId,
            int endId,
            String type);

    void unboundRelationshipValue(PackstreamBuf buf, String elementId, int id, String type);
}
