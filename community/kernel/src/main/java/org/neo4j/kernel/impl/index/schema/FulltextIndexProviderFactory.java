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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexProvider;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.directoryFactory;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

public class FulltextIndexProviderFactory extends AbstractIndexProviderFactory<FulltextIndexProvider>
{
    private static final String KEY = "fulltext";
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( KEY, "1.0" );

    @Override
    protected Class<?> loggingClass()
    {
        return FulltextIndexProvider.class;
    }

    @Override
    public IndexProviderDescriptor descriptor()
    {
        return DESCRIPTOR;
    }

    @Override
    protected FulltextIndexProvider internalCreate( PageCache pageCache, FileSystemAbstraction fs, Monitors monitors, String monitorTag,
                                                    Config config, DatabaseReadOnlyChecker readOnlyDatabaseChecker,
                                                    RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, DatabaseLayout databaseLayout,
                                                    PageCacheTracer pageCacheTracer, Log log, TokenHolders tokenHolders,
                                                    JobScheduler scheduler )
    {
        boolean ephemeral = config.get( GraphDatabaseInternalSettings.ephemeral_lucene );
        DirectoryFactory directoryFactory = directoryFactory( ephemeral );
        IndexDirectoryStructure.Factory directoryStructureFactory = subProviderDirectoryStructure( databaseLayout.databaseDirectory() );
        return new FulltextIndexProvider( DESCRIPTOR, directoryStructureFactory, fs, config, tokenHolders,
                                          directoryFactory, readOnlyDatabaseChecker, scheduler, log );
    }

    private static IndexDirectoryStructure.Factory subProviderDirectoryStructure( Path storeDir )
    {
        return directoriesByProvider( storeDir );
    }
}
