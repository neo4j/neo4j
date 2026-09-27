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
package org.neo4j.storageengine.api;

/**
 * @param includeAtomicStoreFiles "atomic" means that the file cannot be e.g. streamed concurrently with a checkpoint,
 * rather has bo be streamed from start to end without concurrent checkpoint being made.
 * @param includeReplayableStoreFiles "replayable" means that the file can be streamed concurrently with a checkpoint
 * @param includeIdFiles whether to include ID generator files
 * @param includeRecoverableFiles this could potentially apply to both "atomic" and "replayable" and dictates
 * whether to include files that can be recovered/rebuilt if missing.
 * @param segmentsFilter filter applied to the segments of store files, so that a listing
 * can be narrowed down to segments that are new or have changed relative to a store copy requester's segment state.
 */
public record StorageFileSelection(
        boolean includeAtomicStoreFiles,
        boolean includeReplayableStoreFiles,
        boolean includeIdFiles,
        boolean includeRecoverableFiles,
        SegmentsFilter segmentsFilter) {
    public StorageFileSelection(
            boolean includeAtomicStoreFiles, boolean includeReplayableStoreFiles, boolean includeIdFiles) {
        this(includeAtomicStoreFiles, includeReplayableStoreFiles, includeIdFiles, true);
    }

    public StorageFileSelection(
            boolean includeAtomicStoreFiles,
            boolean includeReplayableStoreFiles,
            boolean includeIdFiles,
            boolean includeRecoverableFiles) {
        this(
                includeAtomicStoreFiles,
                includeReplayableStoreFiles,
                includeIdFiles,
                includeRecoverableFiles,
                SegmentsFilter.ALL);
    }
}
