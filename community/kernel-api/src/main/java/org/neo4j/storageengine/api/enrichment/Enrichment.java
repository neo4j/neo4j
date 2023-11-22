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
package org.neo4j.storageengine.api.enrichment;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.memory.MemoryTracker;

/**
 * Describes the enrichment data of a transaction. The 2 forms of enrichment are:
 * <ul>
 *  <li>when writing, the data will be more dynamic in nature as the size will not be known upfront.</li>
 *  <li>when reading, the size of the data will be known so the buffers that contain it can be sized accordingly.</li>
 * </ul>
 */
public abstract sealed class Enrichment {

    /**
     * A representation of some enrichment data for reading. The object has some {@link TxMetadata} associated with it
     * and 5 data {@link ByteBuffer buffers}, representing the entities that changed, the details around those
     * entities, the changes to those entities, the values that may have changed and (optionally) any user metadata
     * associated with the transaction.
     */
    public static final class Read extends Enrichment implements AutoCloseable {

        private final KernelVersion kernelVersion;
        private final int entityCount;
        // contains pointers to the entity details
        private final ByteBuffer entitiesBuffer;
        // contains details about the type of entity and pointers to changes that occurred to it
        private final ByteBuffer detailsBuffer;
        // describes the changes to an entity and pointers to the values that changed
        private final ByteBuffer changesBuffer;
        // contains serialized value objects
        private final ByteBuffer valuesBuffer;

        private final ByteBuffer userMetadataBuffer;
        private final MemoryTracker memoryTracker;

        private Read(
                KernelVersion kernelVersion,
                TxMetadata metadata,
                int entityCount,
                ByteBuffer entitiesBuffer,
                ByteBuffer detailsBuffer,
                ByteBuffer changesBuffer,
                ByteBuffer valuesBuffer,
                ByteBuffer userMetadataBuffer,
                MemoryTracker memoryTracker) {
            super(metadata);
            this.kernelVersion = kernelVersion;
            this.entityCount = entityCount;
            this.entitiesBuffer = entitiesBuffer;
            this.detailsBuffer = detailsBuffer;
            this.changesBuffer = changesBuffer;
            this.valuesBuffer = valuesBuffer;
            this.userMetadataBuffer = userMetadataBuffer;
            this.memoryTracker = memoryTracker;
        }

        /**
         *
         * @param channel the channel to read the enrichment data from
         * @param memoryTracker for tracking memory of the enrichment data
         * @return the enrichment for reading
         * @throws IOException if unable to read the enrichment data
         */
        public static Enrichment.Read deserialize(
                KernelVersion kernelVersion, ReadableChannel channel, MemoryTracker memoryTracker) throws IOException {
            final var txMetadata = TxMetadata.deserialize(channel);

            final var entitiesSize = channel.getInt();
            final var detailsSize = channel.getInt();
            final var changesSize = channel.getInt();
            final var valuesSize = channel.getInt();

            var userMetadataSize = 0;
            if (kernelVersion.isAtLeast(KernelVersion.VERSION_CDC_USER_METADATA_INTRODUCED)) {
                userMetadataSize = channel.getInt();
            }

            return new Enrichment.Read(
                    kernelVersion,
                    txMetadata,
                    entitiesSize / Integer.BYTES,
                    readIntoBuffer(channel, entitiesSize, memoryTracker),
                    readIntoBuffer(channel, detailsSize, memoryTracker),
                    readIntoBuffer(channel, changesSize, memoryTracker),
                    readIntoBuffer(channel, valuesSize, memoryTracker),
                    readUserMetadataBuffer(channel, userMetadataSize, memoryTracker),
                    memoryTracker);
        }

        private static ByteBuffer readIntoBuffer(ReadableChannel channel, int size, MemoryTracker memoryTracker)
                throws IOException {
            memoryTracker.allocateHeap(size);
            final var buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
            final var read = channel.read(buffer);
            assert read == size : "Unable to read all the expected data into the buffer";
            return buffer.flip();
        }

        private static ByteBuffer readUserMetadataBuffer(ReadableChannel channel, int size, MemoryTracker memoryTracker)
                throws IOException {
            if (size <= 0) {
                return null;
            }

            return readIntoBuffer(channel, size, memoryTracker);
        }

        private static ByteBuffer slice(ByteBuffer buffer) {
            return buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
        }

        /**
         * @return the {@link KernelVersion} of the database when the enrichment was created
         */
        public KernelVersion kernelVersion() {
            return kernelVersion;
        }

        /**
         * @return the number of entity changes described in this enrichment
         */
        public int numberOfEntities() {
            return entityCount;
        }

        public ByteBuffer entities() {
            return slice(entitiesBuffer);
        }

        public ByteBuffer entityDetails() {
            return slice(detailsBuffer);
        }

        public ByteBuffer entityChanges() {
            return slice(changesBuffer);
        }

        public ByteBuffer values() {
            return slice(valuesBuffer);
        }

        public Optional<ByteBuffer> userMetadata() {
            if (userMetadataBuffer == null) {
                return Optional.empty();
            }

            return Optional.of(slice(userMetadataBuffer));
        }

        @Override
        public void serialize(WritableChannel channel) throws IOException {
            metadata.serialize(channel);
            // write out the sizes making it easy(er) to skip the actual content
            channel.putInt(entitiesBuffer.capacity())
                    .putInt(detailsBuffer.capacity())
                    .putInt(changesBuffer.capacity())
                    .putInt(valuesBuffer.capacity());

            if (kernelVersion.isAtLeast(KernelVersion.VERSION_CDC_USER_METADATA_INTRODUCED)) {
                channel.putInt(userMetadataBuffer == null ? 0 : userMetadataBuffer.capacity());
            }

            // now write out all the buffer's content from the start
            channel.putAll(slice(entitiesBuffer))
                    .putAll(slice(detailsBuffer))
                    .putAll(slice(changesBuffer))
                    .putAll(slice(valuesBuffer));

            if (userMetadataBuffer != null) {
                channel.putAll(slice(userMetadataBuffer));
            }
        }

        /**
         * Release the memory associated with this enrichment data
         */
        @Override
        public void close() {
            memoryTracker.releaseHeap(entitiesBuffer.capacity());
            memoryTracker.releaseHeap(detailsBuffer.capacity());
            memoryTracker.releaseHeap(changesBuffer.capacity());
            memoryTracker.releaseHeap(valuesBuffer.capacity());
            if (userMetadataBuffer != null) {
                memoryTracker.releaseHeap(userMetadataBuffer.capacity());
            }
        }
    }

    /**
     * A representation of some enrichment data for writing. The data is internally captured in 4
     * {@link WriteEnrichmentChannel channels} representing the entities that changed, the details around those
     * entities, the changes to those entities, the values that may have changed and (optionally) any user metadata
     * associated with the transaction.
     */
    public static final class Write extends Enrichment implements AutoCloseable {

        private final WriteEnrichmentChannel entities;
        private final WriteEnrichmentChannel details;
        private final WriteEnrichmentChannel changes;
        private final WriteEnrichmentChannel values;
        private final WriteEnrichmentChannel userMetadata;

        private Write(
                TxMetadata metadata,
                WriteEnrichmentChannel entities,
                WriteEnrichmentChannel details,
                WriteEnrichmentChannel changes,
                WriteEnrichmentChannel values,
                WriteEnrichmentChannel userMetadata) {
            super(metadata);
            this.entities = requireNonNull(entities);
            this.details = requireNonNull(details);
            this.changes = requireNonNull(changes);
            this.values = requireNonNull(values);
            this.userMetadata = userMetadata;
        }

        /**
         * For writing enrichment data BEFORE {@link KernelVersion#VERSION_CDC_USER_METADATA_INTRODUCED}
         */
        public static Write createV5_8(
                TxMetadata metadata,
                WriteEnrichmentChannel entities,
                WriteEnrichmentChannel details,
                WriteEnrichmentChannel changes,
                WriteEnrichmentChannel values) {
            return new Write(metadata, entities, details, changes, values, null);
        }

        /**
         * For writing enrichment data AT OR AFTER {@link KernelVersion#VERSION_CDC_USER_METADATA_INTRODUCED}
         */
        public static Write createV5_12(
                TxMetadata metadata,
                WriteEnrichmentChannel entities,
                WriteEnrichmentChannel details,
                WriteEnrichmentChannel changes,
                WriteEnrichmentChannel values,
                WriteEnrichmentChannel userMetadata) {
            return new Write(metadata, entities, details, changes, values, userMetadata);
        }

        /**
         * @return the expected size (in bytes) that will be written when the enrichment is serialized
         */
        public long totalSize() {
            final var includeUserMetadata = userMetadata != null;
            final var sizesFactor = includeUserMetadata ? 5 : 4;
            return (Integer.BYTES * sizesFactor)
                    + entities.size()
                    + details.size()
                    + changes.size()
                    + values.size()
                    + (includeUserMetadata ? userMetadata.size() : 0);
        }

        @Override
        public void serialize(WritableChannel channel) throws IOException {
            final var includeUserMetadata = userMetadata != null;

            metadata.serialize(channel);
            // write out the sizes making it easy(er) to skip the actual content
            channel.putInt(entities.size());
            channel.putInt(details.size());
            channel.putInt(changes.size());
            channel.putInt(values.size());
            if (includeUserMetadata) {
                channel.putInt(userMetadata.size());
            }
            // now write out the content
            entities.serialize(channel);
            details.serialize(channel);
            changes.serialize(channel);
            values.serialize(channel);
            if (includeUserMetadata) {
                userMetadata.serialize(channel);
            }
        }

        /**
         * Release the memory associated with this enrichment data
         */
        @Override
        public void close() {
            entities.close();
            details.close();
            changes.close();
            values.close();
            if (userMetadata != null) {
                userMetadata.close();
            }
        }
    }

    protected final TxMetadata metadata;

    protected Enrichment(TxMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * @param channel the channel to write the enrichment data to
     * @throws IOException if unable to write the enrichment data
     */
    public abstract void serialize(WritableChannel channel) throws IOException;

    /**
     * @return the {@link TxMetadata} associated with the enriched transaction
     */
    public TxMetadata metadata() {
        return metadata;
    }
}
