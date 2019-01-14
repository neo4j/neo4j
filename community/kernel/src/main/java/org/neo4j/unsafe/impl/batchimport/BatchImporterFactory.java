/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.util.NoSuchElementException;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

public abstract class BatchImporterFactory extends Service
{
    private final int priority;

    protected BatchImporterFactory( String key, int priority )
    {
        super( key );
        this.priority = priority;
    }

    public abstract BatchImporter instantiate( File storeDir, FileSystemAbstraction fileSystem, PageCache externalPageCache,
            Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats, ImportLogic.Monitor monitor );

    public static BatchImporterFactory withHighestPriority()
    {
        Iterable<BatchImporterFactory> candidates = Service.load( BatchImporterFactory.class );
        BatchImporterFactory highestPrioritized = null;
        for ( BatchImporterFactory candidate : candidates )
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
