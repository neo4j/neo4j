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
package org.neo4j.bolt.protocol.common.message.request.authentication;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;

public final class HelloMessage implements AuthenticationMessage {

    public static final byte SIGNATURE = 0x01;

    private final String userAgent;
    private final List<Feature> features;
    private final RoutingContext routingContext;
    private final Map<String, Object> authToken;
    private final NotificationsConfig notificationsConfig;
    private final Map<String, String> boltAgent;

    public HelloMessage(
            String userAgent,
            List<Feature> features,
            RoutingContext routingContext,
            Map<String, Object> authToken,
            NotificationsConfig notificationsConfig,
            Map<String, String> boltAgent) {
        this.userAgent = userAgent;
        this.features = features;
        this.routingContext = routingContext;
        this.authToken = authToken;
        this.notificationsConfig = notificationsConfig;
        this.boltAgent = boltAgent;
    }

    public HelloMessage(
            String userAgent, List<Feature> features, RoutingContext routingContext, Map<String, Object> authToken) {
        this(userAgent, features, routingContext, authToken, null, null);
    }

    @Override
    public Map<String, Object> authToken() {
        return authToken;
    }

    public RoutingContext routingContext() {
        return routingContext;
    }

    public String userAgent() {
        return this.userAgent;
    }

    public Map<String, String> boltAgent() {
        return this.boltAgent;
    }

    public List<Feature> features() {
        return this.features;
    }

    public NotificationsConfig notificationsConfig() {
        return this.notificationsConfig;
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
                && Objects.equals(boltAgent, that.boltAgent)
                && Objects.equals(features, that.features)
                && Objects.equals(authToken, that.authToken)
                && Objects.equals(routingContext, that.routingContext)
                && Objects.equals(notificationsConfig, that.notificationsConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userAgent, features, authToken, routingContext);
    }

    @Override
    public String toString() {
        var notifications = notificationsConfig != null ? notificationsConfig.toString() : "null";
        return "HelloMessage{" + "userAgent='"
                + userAgent + '\'' + ", features="
                + features + ", boltAgent="
                + boltAgent + ", authToken="
                + authToken + ", routingContext="
                + routingContext + ", notificationsConfig="
                + notifications + '}';
    }
}
