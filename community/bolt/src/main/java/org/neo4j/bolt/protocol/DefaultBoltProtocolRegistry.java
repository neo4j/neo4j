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
package org.neo4j.bolt.protocol;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.BoltProtocol;

public class DefaultBoltProtocolRegistry implements BoltProtocolRegistry {
    private final List<BoltProtocol> protocols;

    private DefaultBoltProtocolRegistry(List<BoltProtocol> protocols) {
        this.protocols = protocols;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public BoltProtocolRegistry.Builder builderOf() {
        return new Builder(this.protocols);
    }

    @Override
    public Optional<BoltProtocol> get(ProtocolVersion protocolVersion) {
        return this.protocols.stream()
                .filter(protocol -> protocolVersion.matches(protocol.version()))
                .max(Comparator.comparing(BoltProtocol::version));
    }

    public static class Builder implements BoltProtocolRegistry.Builder {
        private final List<BoltProtocol> protocols;

        private Builder() {
            this.protocols = new ArrayList<>();
        }

        private Builder(List<BoltProtocol> protocols) {
            this.protocols = new ArrayList<>(protocols);
        }

        @Override
        public DefaultBoltProtocolRegistry build() {
            return new DefaultBoltProtocolRegistry(List.copyOf(this.protocols));
        }

        @Override
        public Builder register(BoltProtocol protocol) {
            // remove any clashing definitions in order to permit overriding of protocol specifications
            this.protocols.removeIf(registered -> registered.version().matches(protocol.version()));

            this.protocols.add(protocol);
            return this;
        }
    }
}
