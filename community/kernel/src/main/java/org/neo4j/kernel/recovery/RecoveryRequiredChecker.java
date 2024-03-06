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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RecoveryState;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;

/**
 * Utility that can determine if a given store will need recovery.
 */
class RecoveryRequiredChecker {
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final StorageEngineFactory storageEngineFactory;
    private final LogTailExtractor logTailExtractor;

    RecoveryRequiredChecker(
            FileSystemAbstraction fs,
            PageCache pageCache,
            Config config,
            StorageEngineFactory storageEngineFactory,
            DatabaseTracers databaseTracers) {
        this.fs = fs;
        this.pageCache = pageCache;
        this.logTailExtractor = new LogTailExtractor(fs, config, storageEngineFactory, databaseTracers);
        this.storageEngineFactory = storageEngineFactory;
    }

    public boolean isRecoveryRequiredAt(DatabaseLayout databaseLayout, MemoryTracker memoryTracker) throws IOException {
        var logTail = logTailExtractor.getTailMetadata(databaseLayout, memoryTracker);
        return isRecoveryRequiredAt(databaseLayout, logTail);
    }

    boolean isRecoveryRequiredAt(DatabaseLayout databaseLayout, LogTailMetadata logTailMetadata) {
        if (!storageEngineFactory.storageExists(fs, databaseLayout)) {
            return false;
        }
        StorageFilesState filesRecoveryState = storageEngineFactory.checkStoreFileState(fs, databaseLayout, pageCache);
        if (filesRecoveryState.recoveryState() != RecoveryState.RECOVERED) {
            return true;
        }
        return logTailMetadata.isRecoveryRequired();
    }
}
