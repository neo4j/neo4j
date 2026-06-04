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
package org.neo4j.bolt.protocol.common;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.neo4j.bolt.fsm.StateMachineConfiguration;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.DefaultMetadataHandler;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.protocol.io.reader.struct.DateReader;
import org.neo4j.bolt.protocol.io.reader.struct.DateTimeReader;
import org.neo4j.bolt.protocol.io.reader.struct.DateTimeZoneIdReader;
import org.neo4j.bolt.protocol.io.reader.struct.DurationReader;
import org.neo4j.bolt.protocol.io.reader.struct.LocalDateTimeReader;
import org.neo4j.bolt.protocol.io.reader.struct.LocalTimeReader;
import org.neo4j.bolt.protocol.io.reader.struct.Point2dReader;
import org.neo4j.bolt.protocol.io.reader.struct.Point3dReader;
import org.neo4j.bolt.protocol.io.reader.struct.TimeReader;
import org.neo4j.bolt.protocol.io.reader.struct.VectorReader;
import org.neo4j.bolt.protocol.io.writer.DefaultVersionedValueWriter;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.bolt.protocol.v41.BoltProtocolV41;
import org.neo4j.bolt.protocol.v42.BoltProtocolV42;
import org.neo4j.bolt.protocol.v43.BoltProtocolV43;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.protocol.v50.BoltProtocolV50;
import org.neo4j.bolt.protocol.v51.BoltProtocolV51;
import org.neo4j.bolt.protocol.v52.BoltProtocolV52;
import org.neo4j.bolt.protocol.v53.BoltProtocolV53;
import org.neo4j.bolt.protocol.v54.BoltProtocolV54;
import org.neo4j.bolt.protocol.v56.BoltProtocolV56;
import org.neo4j.bolt.protocol.v57.BoltProtocolV57;
import org.neo4j.bolt.protocol.v58.BoltProtocolV58;
import org.neo4j.bolt.protocol.v60.BoltProtocolV60;
import org.neo4j.bolt.protocol.v61.BoltProtocolV61;
import org.neo4j.boltmessages.request.RequestMessage;
import org.neo4j.boltmessages.response.ResponseMessage;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.values.storable.Value;

public interface BoltProtocol {

    static List<BoltProtocol> installed() {
        return List.of(
                BoltProtocolV40.getInstance(),
                BoltProtocolV41.getInstance(),
                BoltProtocolV42.getInstance(),
                BoltProtocolV43.getInstance(),
                BoltProtocolV44.getInstance(),
                BoltProtocolV50.getInstance(),
                BoltProtocolV51.getInstance(),
                BoltProtocolV52.getInstance(),
                BoltProtocolV53.getInstance(),
                BoltProtocolV54.getInstance(),
                BoltProtocolV56.getInstance(),
                BoltProtocolV57.getInstance(),
                BoltProtocolV58.getInstance(),
                BoltProtocolV60.getInstance(),
                BoltProtocolV61.getInstance());
    }

    static String latestVersionInstalled() {
        return latestInstalled().version().toString();
    }

    static BoltProtocol latestInstalled() {
        return installed().stream()
                .max(Comparator.comparing(BoltProtocol::version))
                .orElseThrow();
    }

    static List<BoltProtocol> available() {
        return installed().stream().filter(it -> !it.preview()).toList();
    }

    /**
     * Identifies the version number via which this protocol implementation is identified during the
     * negotiation process.
     *
     * @return a protocol version number.
     */
    ProtocolVersion version();

    /**
     * Indicates whether this protocol version is currently a preview-release (e.g. will be
     * unavailable unless explicitly selected via configuration options).
     *
     * @return true if preview, false otherwise.
     */
    default boolean preview() {
        return false;
    }

    /**
     * Retrieves a set of features which are always enabled on this connection regardless of whether
     * they have been negotiated or not.
     * <p/>
     * Note: Features listed within this set are effectively blacklisted (e.g. cannot be negotiated
     * later) and should thus be provided through reader/writer pipeline configurators within the
     * protocol implementations rather than relying on the configuration functions provided by
     * {@link Feature}.
     *
     * @return a set of features.
     */
    default Set<Feature> features() {
        return Set.of(Feature.UTC_DATETIME);
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

    StateMachineConfiguration stateMachine();

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
     * Registers protocol specific struct readers for decoding of values sent to the server by a
     * client.
     *
     * @param builder a struct registry.
     */
    default void registerStructReaders(StructRegistry.Builder<Connection, Value> builder) {
        builder.register(DateReader.getInstance())
                .register(DurationReader.getInstance())
                .register(LocalDateTimeReader.getInstance())
                .register(LocalTimeReader.getInstance())
                .register(Point2dReader.getInstance())
                .register(Point3dReader.getInstance())
                .register(TimeReader.getInstance())
                .register(DateTimeReader.getInstance())
                .register(DateTimeZoneIdReader.getInstance())
                .register(VectorReader.getInstance());
    }

    /**
     * Registers protocol specific struct writers for encoding of values through the result streaming
     * APIs.
     *
     * @param pipeline a writer pipeline.
     */
    default void registerStructWriters(WriterPipeline pipeline) {
        pipeline.addLast(DefaultVersionedValueWriter.getInstance());
    }

    /**
     * Checks whether a given Packstream value type is supported by this protocol version.
     *
     * @param type a Packstream type.
     * @return true if supported, false otherwise.
     */
    default boolean supportsPackstreamType(Type type) {
        return true;
    }

    /**
     * Retrieves a list of Packstream value types supported by this protocol version.
     *
     * @return a list of Packstream value types.
     */
    default List<Type> supportedPackstreamTypes() {
        return Type.VALID_TYPES.stream().filter(this::supportsPackstreamType).toList();
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
