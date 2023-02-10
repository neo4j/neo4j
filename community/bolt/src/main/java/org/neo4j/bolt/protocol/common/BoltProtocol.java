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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.DefaultMetadataHandler;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.protocol.io.writer.DefaultStructWriter;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.bolt.protocol.v41.BoltProtocolV41;
import org.neo4j.bolt.protocol.v42.BoltProtocolV42;
import org.neo4j.bolt.protocol.v43.BoltProtocolV43;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.protocol.v50.BoltProtocolV50;
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.storable.Value;

public interface BoltProtocol {

    static List<BoltProtocol> available(
            LogService logging,
            BoltGraphDatabaseManagementServiceSPI databaseManagementServiceSPI,
            SystemNanoClock clock) {
        return List.of(
                new BoltProtocolV40(logging, databaseManagementServiceSPI, clock),
                new BoltProtocolV41(logging, databaseManagementServiceSPI, clock),
                new BoltProtocolV42(logging, databaseManagementServiceSPI, clock),
                new BoltProtocolV43(logging, databaseManagementServiceSPI, clock),
                new BoltProtocolV44(logging, databaseManagementServiceSPI, clock),
                new BoltProtocolV50(logging, databaseManagementServiceSPI, clock));
    }

    /**
     * Identifies the version number via which this protocol implementation is identified during the negotiation process.
     *
     * @return a protocol version number.
     */
    ProtocolVersion version();

    /**
     * Retrieves a set of features which are always enabled on this connection regardless of whether they have been
     * negotiated or not.
     * <p />
     * Note: Features listed within this set are effectively blacklisted (e.g. cannot be negotiated later) and should
     * thus be provided through reader/writer pipeline configurators within the protocol implementations rather than
     * relying on the configuration functions provided by {@link Feature}.
     *
     * @return a set of features.
     */
    default Set<Feature> features() {
        return Collections.emptySet();
    }

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

    StateMachine createStateMachine(Connection connection);

    /**
     * Retrieves the struct registry which provides read capabilities for request messages.
     *
     * @return a struct registry.
     */
    StructRegistry<Connection, RequestMessage> requestMessageRegistry();

    /**
     * Retrieves the struct registry which provides write capabilities for response messages.
     *
     * @return a struct registry.
     */
    StructRegistry<Connection, ResponseMessage> responseMessageRegistry();

    /**
     * Registers protocol specific struct readers for decoding of values sent to the server by a client.
     *
     * @param builder a struct registry.
     */
    void registerStructReaders(StructRegistry.Builder<Connection, Value> builder);

    /**
     * Registers protocol specific struct writers for encoding of values through the result streaming APIs.
     *
     * @param pipeline a writer pipeline.
     */
    default void registerStructWriters(WriterPipeline pipeline) {
        pipeline.addLast(DefaultStructWriter.getInstance());
    }

    /**
     * Retrieves a metadata handler which generates metadata entries within operation responses.
     *
     * @return a metadata handler implementation.
     */
    default MetadataHandler metadataHandler() {
        return DefaultMetadataHandler.getInstance();
    }
}
