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
package org.neo4j.kernel.impl.store.segment;

/**
 * Service to tracks which segments of store files have been modified at which checkpoint
 * append indexes, enabling efficient incremental backups and segment-level recovery operations.
 * The metadata maintains a mapping between file segments and the checkpoint append index at
 * which they were last modified.
 */
public interface SegmentMetadataService {

    SegmentMetadataService EMPTY_METADATA_SERVICE = new SegmentMetadataService() {
        @Override
        public void storeMetadata(long checkpointAppendIndex) {}

        @Override
        public SegmentsMetadata segmentsMetadata() {
            return SegmentsMetadata.EMPTY_SEGMENTS_METADATA;
        }

        @Override
        public boolean trackedSegmentedStore() {
            return false;
        }
    };

    void storeMetadata(long checkpointAppendIndex);

    SegmentsMetadata segmentsMetadata();

    boolean trackedSegmentedStore();
}
