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
import org.neo4j.kernel.impl.index.schema.NumberIndexProvider;
import org.neo4j.kernel.impl.index.schema.SpatialIndexProvider;
import org.neo4j.kernel.impl.index.schema.TemporalIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSlotSelector10;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;

@Service.Implementation( KernelExtensionFactory.class )
public class NativeLuceneFusionIndexProviderFactory10 extends
        NativeLuceneFusionIndexProviderFactory<NativeLuceneFusionIndexProviderFactory10.Dependencies>
{
    public static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( KEY, "1.0" );
    static final int PRIORITY = LuceneIndexProvider.PRIORITY + 1;

    public interface Dependencies extends LuceneIndexProviderFactory.Dependencies
    {
    }

    @Override
    public FusionIndexProvider newInstance( KernelContext context, Dependencies dependencies )
    {
        PageCache pageCache = dependencies.pageCache();
        File storeDir = context.storeDir();
        FileSystemAbstraction fs = dependencies.fileSystem();
        Log log = dependencies.getLogService().getInternalLogProvider().getLog( FusionIndexProvider.class );
        Monitors monitors = dependencies.monitors();
        monitors.addMonitorListener( new LoggingMonitor( log ), DESCRIPTOR.toString() );
        IndexProvider.Monitor monitor = monitors.newMonitor( IndexProvider.Monitor.class, DESCRIPTOR.toString() );
        Config config = dependencies.getConfig();
        OperationalMode operationalMode = context.databaseInfo().operationalMode;
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = dependencies.recoveryCleanupWorkCollector();
        return create( pageCache, storeDir, fs, monitor, config, operationalMode, recoveryCleanupWorkCollector );
    }

    public static FusionIndexProvider create( PageCache pageCache, File storeDir, FileSystemAbstraction fs,
                                                   IndexProvider.Monitor monitor, Config config, OperationalMode operationalMode,
                                                   RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        IndexDirectoryStructure.Factory childDirectoryStructure = subProviderDirectoryStructure( storeDir );
        boolean readOnly = IndexProviderFactoryUtil.isReadOnly( config, operationalMode );
        boolean archiveFailedIndex = config.get( GraphDatabaseSettings.archive_failed_index );

        NumberIndexProvider number =
                IndexProviderFactoryUtil.numberProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
        SpatialIndexProvider spatial =
                IndexProviderFactoryUtil.spatialProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly, config );
        TemporalIndexProvider temporal =
                IndexProviderFactoryUtil.temporalProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
        LuceneIndexProvider lucene = IndexProviderFactoryUtil.luceneProvider( fs, childDirectoryStructure, monitor, config, operationalMode );

        String defaultSchemaProvider = config.get( GraphDatabaseSettings.default_schema_provider );
        int priority = PRIORITY;
        if ( GraphDatabaseSettings.SchemaIndex.NATIVE10.providerName().equals( defaultSchemaProvider ) )
        {
            priority = 100;
        }
        return new FusionIndexProvider( EMPTY, number, spatial, temporal, lucene, new FusionSlotSelector10(),
                DESCRIPTOR, priority, directoriesByProvider( storeDir ), fs, archiveFailedIndex );
    }

    private static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir )
    {
        return NativeLuceneFusionIndexProviderFactory.subProviderDirectoryStructure( storeDir, DESCRIPTOR );
    }
}
