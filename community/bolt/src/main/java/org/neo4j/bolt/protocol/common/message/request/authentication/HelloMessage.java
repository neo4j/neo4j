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
package org.neo4j.bolt.protocol.common.message.request.authentication;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;

public final class HelloMessage implements RequestMessage {

    public static final byte SIGNATURE = 0x01;

    private final String userAgent;
    private final List<Feature> features;
    private final RoutingContext routingContext;
    private final Map<String, Object> authToken;

    public HelloMessage(
            String userAgent, List<Feature> features, RoutingContext routingContext, Map<String, Object> authToken) {
        this.userAgent = userAgent;
        this.features = features;
        this.routingContext = routingContext;
        this.authToken = authToken;
    }

    public HelloMessage(String userAgent, List<Feature> features, RoutingContext routingContext) {
        this(userAgent, features, routingContext, null);
    }

    public Map<String, Object> authToken() {
        return authToken;
    }

    public RoutingContext routingContext() {
        return routingContext;
    }

    public String userAgent() {
        return this.userAgent;
    }

    public List<Feature> features() {
        return this.features;
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
        HelloMessage that = (HelloMessage) o;
        return Objects.equals(userAgent, that.userAgent)
                && Objects.equals(features, that.features)
                && Objects.equals(authToken, that.authToken)
                && Objects.equals(routingContext, that.routingContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userAgent, features, authToken, routingContext);
    }

    @Override
    public String toString() {
        return "HelloMessage{" + "userAgent='"
                + userAgent + '\'' + ", features="
                + features + ", authToken="
                + authToken + ", routingContext="
                + routingContext + '}';
    }
}
