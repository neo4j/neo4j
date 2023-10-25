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
package org.neo4j.bolt.protocol.common.connection.hint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.values.virtual.MapValueBuilder;

public final class ConnectionHintRegistry {
    private final List<ConnectionHintProvider> providers;

    private ConnectionHintRegistry(List<ConnectionHintProvider> providers) {
        this.providers = providers;
    }

    public static ConnectionHintRegistry.Builder newBuilder() {
        return new ConnectionHintRegistry.Builder();
    }

    public void applyTo(ProtocolVersion version, MapValueBuilder builder) {
        this.providers.stream()
                .filter(it -> version.isAtLeast(it.supportedSince()) && version.isAtMost(it.supportedUntil()))
                .filter(it -> it.isApplicable())
                .forEach(it -> it.append(builder));
    }

    public static final class Builder {
        private final List<ConnectionHintProvider> providers = new ArrayList<>();

        private Builder() {}

        public ConnectionHintRegistry build() {
            return new ConnectionHintRegistry(new ArrayList<>(this.providers));
        }

        public Builder withProvider(ConnectionHintProvider provider) {
            this.providers.add(provider);
            return this;
        }

        public Builder withProviders(ConnectionHintProvider... providers) {
            this.providers.addAll(Arrays.asList(providers));
            return this;
        }
    }
}
