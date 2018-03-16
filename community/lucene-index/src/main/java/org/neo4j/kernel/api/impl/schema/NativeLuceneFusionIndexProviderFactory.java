/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSelector10;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;

@Service.Implementation( KernelExtensionFactory.class )
public class NativeLuceneFusionIndexProviderFactory
        extends KernelExtensionFactory<NativeLuceneFusionIndexProviderFactory.Dependencies>
{
    public static final String KEY = LuceneIndexProviderFactory.KEY + "+" + NumberIndexProvider.KEY;
    private static final int PRIORITY = LuceneIndexProvider.PRIORITY + 1;

    public static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( KEY, "1.0" );

    public interface Dependencies extends LuceneIndexProviderFactory.Dependencies
    {
        PageCache pageCache();

        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector();
    }

    public NativeLuceneFusionIndexProviderFactory()
    {
        super( KEY );
    }

    @Override
    public FusionIndexProvider newInstance( KernelContext context, Dependencies dependencies )
    {
        PageCache pageCache = dependencies.pageCache();
        File storeDir = context.storeDir();
        FileSystemAbstraction fs = dependencies.fileSystem();
        Log log = dependencies.getLogService().getInternalLogProvider().getLog( FusionIndexProvider.class );
        Monitors monitors = dependencies.monitors();
        monitors.addMonitorListener( new LoggingMonitor( log ), KEY );
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
        IndexDirectoryStructure.Factory childDirectoryStructure = subProviderDirectoryStructure( storeDir );
        boolean readOnly = isReadOnly( config, operationalMode );

        // This is the v1.0 of the lucene+native fusion setup where there's only number+lucene indexes
        NumberIndexProvider numberProvider =
                new NumberIndexProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
        LuceneIndexProvider luceneProvider = LuceneIndexProviderFactory.create( fs, childDirectoryStructure, monitor, config, operationalMode );

        boolean useNativeIndex = config.get( GraphDatabaseSettings.enable_native_schema_index );
        int priority = useNativeIndex ? PRIORITY : 0;
        return new FusionIndexProvider( EMPTY, numberProvider,
                EMPTY, EMPTY, luceneProvider, new FusionSelector10(), DESCRIPTOR, priority, directoriesByProvider( storeDir ), fs );
    }

    public static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir )
    {
        IndexDirectoryStructure parentDirectoryStructure = directoriesByProvider( storeDir ).forProvider( DESCRIPTOR );
        return directoriesBySubProvider( parentDirectoryStructure );
    }

    private static boolean isReadOnly( Config config, OperationalMode operationalMode )
    {
        return config.get( GraphDatabaseSettings.read_only ) && (OperationalMode.single == operationalMode);
    }
}
