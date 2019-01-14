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
package org.neo4j.kernel.api.impl.schema;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.SpatialIndexProvider;
import org.neo4j.kernel.impl.index.schema.TemporalIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSlotSelector00;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProviderKey;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;

@Service.Implementation( KernelExtensionFactory.class )
public class LuceneIndexProviderFactory extends
        KernelExtensionFactory<LuceneIndexProviderFactory.Dependencies>
{
    public static final String KEY = "lucene";

    public static final IndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new IndexProvider.Descriptor( KEY, "1.0" );

    public interface Dependencies
    {
        PageCache pageCache();

        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector();

        Config getConfig();

        Monitors monitors();

        LogService getLogService();

        FileSystemAbstraction fileSystem();
    }

    public LuceneIndexProviderFactory()
    {
        super( KEY );
    }

    @Override
    public IndexProvider newInstance( KernelContext context, Dependencies dependencies )
    {
        PageCache pageCache = dependencies.pageCache();
        File storeDir = context.storeDir();
        FileSystemAbstraction fs = dependencies.fileSystem();
        Monitors monitors = dependencies.monitors();
        Log log = dependencies.getLogService().getInternalLogProvider().getLog( LuceneIndexProvider.class );
        monitors.addMonitorListener( new LoggingMonitor( log ), PROVIDER_DESCRIPTOR.toString() );
        IndexProvider.Monitor monitor = monitors.newMonitor( IndexProvider.Monitor.class, KEY );
        Config config = dependencies.getConfig();
        OperationalMode operationalMode = context.databaseInfo().operationalMode;
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = dependencies.recoveryCleanupWorkCollector();
        return newInstance( pageCache, storeDir, fs, monitor, config, operationalMode, recoveryCleanupWorkCollector );
    }

    public static FusionIndexProvider newInstance( PageCache pageCache, File storeDir, FileSystemAbstraction fs,
            IndexProvider.Monitor monitor, Config config, OperationalMode operationalMode,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        boolean readOnly = IndexProviderFactoryUtil.isReadOnly( config, operationalMode );
        boolean archiveFailedIndex = config.get( GraphDatabaseSettings.archive_failed_index );
        IndexDirectoryStructure.Factory luceneDirStructure = directoriesByProviderKey( storeDir );
        IndexDirectoryStructure.Factory childDirectoryStructure = subProviderDirectoryStructure( storeDir );

        LuceneIndexProvider lucene = IndexProviderFactoryUtil.luceneProvider( fs, luceneDirStructure, monitor, config, operationalMode );
        TemporalIndexProvider temporal =
                IndexProviderFactoryUtil.temporalProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
        SpatialIndexProvider spatial =
                IndexProviderFactoryUtil.spatialProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly, config );

        String defaultSchemaProvider = config.get( GraphDatabaseSettings.default_schema_provider );
        int priority = LuceneIndexProvider.PRIORITY;
        if ( GraphDatabaseSettings.SchemaIndex.LUCENE10.providerName().equals( defaultSchemaProvider ) )
        {
            priority = 100;
        }
        return new FusionIndexProvider( EMPTY, EMPTY, spatial, temporal, lucene, new FusionSlotSelector00(),
                PROVIDER_DESCRIPTOR, priority, directoriesByProvider( storeDir ), fs, archiveFailedIndex );
    }

    private static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir )
    {
        return NativeLuceneFusionIndexProviderFactory.subProviderDirectoryStructure( storeDir, PROVIDER_DESCRIPTOR );
    }

}
