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

import java.util.Optional;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.BoltProtocol;

/**
 * Represents a component that instantiates Bolt protocol handlers.
 *
 * @see BoltProtocol
 */
public interface BoltProtocolRegistry {

    /**
     * Provides a factory for protocol registry instances.
     */
    static Builder builder() {
        return DefaultBoltProtocolRegistry.builder();
    }

    /**
     * Retrieves a builder consisting of the protocol versions within this registry.
     *
     * @return a builder.
     */
    Builder builderOf();

    /**
     * Retrieves a protocol specification for a Bolt protocol with a matching version.
     * <p>
     * When no protocol definition for the desired version is registered, {@code null} is returned instead.
     *
     * @param protocolVersion the version as negotiated by the initial handshake.
     * @return new protocol handler when given protocol version is known and valid, {@code null} otherwise.
     */
    Optional<BoltProtocol> get(ProtocolVersion protocolVersion);

    interface Builder {

        /**
         * Creates a new immutable protocol registry using the set of protocols registered within this builder.
         *
         * @return a protocol registry.
         */
        BoltProtocolRegistry build();

        /**
         * Registers a new protocol definition with this builder.
         *
         * @param protocol a protocol definition.
         * @return a reference to this builder.
         */
        Builder register(BoltProtocol protocol);

        /**
         * Registers multiple protocol definitions with this builder.
         * @param protocols a collection of protocol definitions.
         * @return a reference to this builder.
         */
        default Builder register(Iterable<BoltProtocol> protocols) {
            protocols.forEach(this::register);
            return this;
        }
    }
}
