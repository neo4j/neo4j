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
package org.neo4j.storageengine.api.enrichment;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

/**
 * Describes the enrichment data of a transaction.
 * Currently, the 2 forms of enrichment are the same. In a future PR they will be different.
 * <ul>
 *  <li>when writing, the data will be more dynamic in nature as the size will not be known upfront.</li>
 *  <li>when reading, the size of the data will be known so the buffers that contain it can be sized accordingly.</li>
 * </ul>
 */
public abstract sealed class Enrichment implements AutoCloseable {

    /**
     * A representation of some enrichment data for reading.
     */
    public static final class Read extends Enrichment {

        private Read(TxMetadata metadata) {
            super(requireNonNull(metadata));
            // future PR will have the fixed buffers required to read the enrichment data
        }

        /**
         *
         * @param channel the channel to read the enrichment data from
         * @return the enrichment for reading
         * @throws IOException if unable to read the enrichment data
         */
        public static Enrichment.Read deserialize(ReadableChannel channel) throws IOException {
            return new Enrichment.Read(TxMetadata.deserialize(channel));
        }

        @Override
        public void serialize(WritableChannel channel) throws IOException {
            throw new IOException("This command is only for reading and contains no enrichment data");
        }

        @Override
        public void close() {
            // currently a no-op
        }
    }

    /**
     * A representation of some enrichment data for writing.
     */
    public static final class Write extends Enrichment {

        public Write(TxMetadata metadata) {
            super(requireNonNull(metadata));
            // future PR will have the dynamic buffers required to capture the enrichment data for writing
        }

        @Override
        public void serialize(WritableChannel channel) throws IOException {
            metadata.serialize(channel);
        }

        @Override
        public void close() {
            // currently a no-op
        }
    }

    protected final TxMetadata metadata;

    protected Enrichment(TxMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Reads the {@link TxMetadata} from the channel, and scans past the rest of the enrichment data
     * @param channel the channel to read from
     * @return the metadata
     * @throws IOException if unable to read the channel
     */
    public static TxMetadata readMetadataAndPastEnrichmentData(ReadableChannel channel) throws IOException {
        // there isn't any enrichment data currently - will be in a future PR
        return TxMetadata.deserialize(channel);
    }

    /**
     * @param channel the channel to write the enrichment data to
     * @throws IOException if unable to write the enrichment data
     */
    public abstract void serialize(WritableChannel channel) throws IOException;

    public abstract void close();

    public TxMetadata metadata() {
        return metadata;
    }
}
