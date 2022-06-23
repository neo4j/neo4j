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
package org.neo4j.bolt.protocol.common;

import java.util.function.Predicate;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connection.BoltConnection;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.protocol.io.LegacyBoltValueWriter;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructRegistry;

public interface BoltProtocol {
    /**
     * Identifies the version number via which this protocol implementation is identified during the negotiation process.
     *
     * @return a protocol version number.
     */
    ProtocolVersion version();

    /**
     * Retrieves a provider which registers a set of connection hints upon connection creation.
     *
     * @return a hint provider.
     */
    default ConnectionHintProvider connectionHintProvider() {
        return ConnectionHintProvider.noop();
    }

    /**
     * Retrieves a predicate which selects the frame signals supported by the protocol version.
     *
     * @return a signal predicate.
     */
    default Predicate<FrameSignal> frameSignalFilter() {
        // filter none of the passed signals (e.g. all features are supported)
        return signal -> false;
    }

    StateMachine createStateMachine(BoltChannel boltChannel);

    /**
     * Retrieves a factory capable of creating a protocol specific value writer for a given buffer.
     *
     * @return a result handler.
     * @deprecated Will be removed in 6.0 - Required for legacy id support.
     */
    // TODO: Deprecation: Intermediate LegacyBoltValueWriter will be merged with DefaultBoltValueWriter in 6.0
    @SuppressWarnings("removal")
    @Deprecated(forRemoval = true)
    default LegacyBoltValueWriter.Factory valueWriterFactory() {
        return LegacyBoltValueWriter::new;
    }

    /**
     * Retrieves the struct registry which provides read capabilities for request messages.
     *
     * @return a struct registry.
     */
    StructRegistry<RequestMessage> requestMessageRegistry(BoltConnection connection);

    /**
     * Retrieves the struct registry which provides write capabilities for response messages.
     *
     * @return a struct registry.
     */
    StructRegistry<ResponseMessage> responseMessageRegistry(BoltConnection connection);
}
