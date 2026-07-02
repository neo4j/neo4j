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

import static java.util.Collections.emptyList;

import java.util.List;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;

/// Holds information about storage files for a specific store.
///
/// A store is either:
/// * [RecoveryState#RECOVERED], and thus doing well
/// * [RecoveryState#RECOVERABLE], also doing well, but requires recovery to fix a few things
///   It may be possible to recover in some situations, such as the creation occurring since the last checkpoint.
/// * [RecoveryState#UNRECOVERABLE], where some stores files are missing or broken, and should be specified in its
///   [#unrecoverableState(java.util.List)] factory method.
public record StorageFilesState(RecoveryState recoveryState, List<StoreFile> missingFiles) {

    public static StorageFilesState recoveredState() {
        return new StorageFilesState(RecoveryState.RECOVERED, emptyList());
    }

    public static StorageFilesState recoverableState() {
        return new StorageFilesState(RecoveryState.RECOVERABLE, emptyList());
    }

    public static StorageFilesState unrecoverableState(List<StoreFile> missingFiles) {
        return new StorageFilesState(RecoveryState.UNRECOVERABLE, missingFiles);
    }
}
