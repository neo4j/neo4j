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

import java.nio.file.Path;
import java.util.Collection;

/**
 * Holds information about storage files for a specific store. Either a store is doing well where e.g. {@link RecoveryState#RECOVERED}
 * or {@link RecoveryState#RECOVERABLE} is used, or some store files are missing or broken and {@link RecoveryState#UNRECOVERABLE} together
 * with a list of missing or broken files can be specified in {@link #unrecoverableState(Collection)}.
 */
public record StorageFilesState(RecoveryState recoveryState, Collection<Path> missingFiles) {

    public static StorageFilesState recoverableState() {
        return new StorageFilesState(RecoveryState.RECOVERABLE, emptyList());
    }

    public static StorageFilesState recoveredState() {
        return new StorageFilesState(RecoveryState.RECOVERED, emptyList());
    }

    public static StorageFilesState unrecoverableState(Collection<Path> missingFiles) {
        return new StorageFilesState(RecoveryState.UNRECOVERABLE, missingFiles);
    }
}
