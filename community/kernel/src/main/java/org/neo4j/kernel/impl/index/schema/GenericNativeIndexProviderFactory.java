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
package org.neo4j.kernel.impl.index.schema;

import java.nio.file.Path;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

@ServiceProvider
public class GenericNativeIndexProviderFactory extends AbstractIndexProviderFactory
{
    public GenericNativeIndexProviderFactory()
    {
        super( GenericNativeIndexProvider.KEY );
    }

    @Override
    protected Class<?> loggingClass()
    {
        return GenericNativeIndexProvider.class;
    }

    @Override
    public IndexProviderDescriptor descriptor()
    {
        return GenericNativeIndexProvider.DESCRIPTOR;
    }

    @Override
    protected GenericNativeIndexProvider internalCreate( PageCache pageCache, Path storeDir, FileSystemAbstraction fs, Monitors monitors,
                                                         String monitorTag, Config config, OperationalMode operationalMode,
                                                         RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer pageCacheTracer )
    {
        return create( pageCache, storeDir, fs, monitors, monitorTag, config, operationalMode, recoveryCleanupWorkCollector, pageCacheTracer );
    }

    public static GenericNativeIndexProvider create( PageCache pageCache, Path storeDir, FileSystemAbstraction fs, Monitors monitors,
                                                     String monitorTag, Config config, OperationalMode mode,
                                                     RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer pageCacheTracer )
    {
        IndexDirectoryStructure.Factory directoryStructure = directoriesByProvider( storeDir );
        boolean readOnly = config.get( GraphDatabaseSettings.read_only ) && (OperationalMode.SINGLE == mode);
        DatabaseIndexContext databaseIndexContext = DatabaseIndexContext.builder( pageCache, fs ).withMonitors( monitors ).withTag( monitorTag )
                                                                        .withReadOnly( readOnly ).withPageCacheTracer( pageCacheTracer )
                                                                        .build();
        return new GenericNativeIndexProvider( databaseIndexContext, directoryStructure, recoveryCleanupWorkCollector, config );
    }
}
