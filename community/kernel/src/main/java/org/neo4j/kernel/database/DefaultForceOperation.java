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
package org.neo4j.kernel.database;

import java.io.IOException;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.dbms.database.DatabasePageCache.FlushGuard;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.storageengine.api.StorageEngine;

public class DefaultForceOperation implements CheckPointerImpl.ForceOperation {
    private final IndexingService indexingService;
    private final StorageEngine storageEngine;
    private final DatabasePageCache databasePageCache;

    public DefaultForceOperation(
            IndexingService indexingService, StorageEngine storageEngine, DatabasePageCache databasePageCache) {
        this.indexingService = indexingService;
        this.storageEngine = storageEngine;
        this.databasePageCache = databasePageCache;
    }

    @Override
    public void flushAndForce(DatabaseFlushEvent databaseFlushEvent, CursorContext cursorContext) throws IOException {
        FlushGuard flushGuard = databasePageCache.flushGuard(databaseFlushEvent);
        indexingService.checkpoint(databaseFlushEvent, cursorContext);
        storageEngine.checkpoint(databaseFlushEvent, cursorContext);
        flushGuard.flushUnflushed();
    }
}
