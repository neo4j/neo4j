/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;

class NativeRelationshipTypeScanStore extends NativeTokenScanStore implements RelationshipTypeScanStore
{
    NativeRelationshipTypeScanStore( PageCache pageCache, DatabaseLayout directoryStructure, FileSystemAbstraction fs,
            FullStoreChangeStream fullStoreChangeStream, boolean readOnly, Config config, Monitors monitors,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, EntityType entityType, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        super( pageCache, directoryStructure, fs, fullStoreChangeStream, readOnly, config, monitors, recoveryCleanupWorkCollector, entityType, cacheTracer,
                memoryTracker, "Relationship Type Scan Store" );
    }
}
