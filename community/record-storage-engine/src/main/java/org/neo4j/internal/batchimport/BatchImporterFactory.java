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
package org.neo4j.internal.batchimport;

import java.util.NoSuchElementException;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.NamedService;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.LogFilesInitializer;

@Service
public abstract class BatchImporterFactory implements NamedService
{
    private final int priority;

    protected BatchImporterFactory( int priority )
    {
        this.priority = priority;
    }

    public abstract BatchImporter instantiate( DatabaseLayout directoryStructure, FileSystemAbstraction fileSystem, PageCache externalPageCache,
            PageCacheTracer pageCacheTracer, Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats, ImportLogic.Monitor monitor,
            JobScheduler jobScheduler, Collector badCollector, LogFilesInitializer logFilesInitializer, MemoryTracker memoryTracker );

    public static BatchImporterFactory withHighestPriority()
    {
        BatchImporterFactory highestPrioritized = null;
        for ( BatchImporterFactory candidate : Services.loadAll( BatchImporterFactory.class ) )
        {
            if ( highestPrioritized == null || candidate.priority > highestPrioritized.priority )
            {
                highestPrioritized = candidate;
            }
        }
        if ( highestPrioritized == null )
        {
            throw new NoSuchElementException( "No batch importers found" );
        }
        return highestPrioritized;
    }
}
