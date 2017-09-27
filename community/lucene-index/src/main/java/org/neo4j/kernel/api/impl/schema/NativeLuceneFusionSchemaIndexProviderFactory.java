/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.NativeSchemaNumberIndexProvider;
import org.neo4j.kernel.impl.index.schema.NativeSelector;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

@Service.Implementation( KernelExtensionFactory.class )
public class NativeLuceneFusionSchemaIndexProviderFactory
        extends KernelExtensionFactory<NativeLuceneFusionSchemaIndexProviderFactory.Dependencies>
{
    public static final String KEY = LuceneSchemaIndexProviderFactory.KEY + "+" + NativeSchemaNumberIndexProvider.KEY;
    private static final int PRIORITY = LuceneSchemaIndexProvider.PRIORITY + 1;

    public static final SchemaIndexProvider.Descriptor DESCRIPTOR = new SchemaIndexProvider.Descriptor( KEY, "1.0" );

    public interface Dependencies extends LuceneSchemaIndexProviderFactory.Dependencies
    {
        PageCache pageCache();

        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector();
    }

    public NativeLuceneFusionSchemaIndexProviderFactory()
    {
        super( KEY );
    }

    @Override
    public FusionSchemaIndexProvider newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        PageCache pageCache = dependencies.pageCache();
        File storeDir = context.storeDir();
        FileSystemAbstraction fs = dependencies.fileSystem();
        Log log = dependencies.getLogService().getInternalLogProvider().getLog( FusionSchemaIndexProvider.class );
        Monitors monitors = dependencies.monitors();
        monitors.addMonitorListener( new LoggingMonitor( log ), KEY );
        SchemaIndexProvider.Monitor monitor = monitors.newMonitor( SchemaIndexProvider.Monitor.class, KEY );
        Config config = dependencies.getConfig();
        OperationalMode operationalMode = context.databaseInfo().operationalMode;
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = dependencies.recoveryCleanupWorkCollector();
        return newInstance( pageCache, storeDir, fs, monitor, config, operationalMode, recoveryCleanupWorkCollector );
    }

    public static FusionSchemaIndexProvider newInstance( PageCache pageCache, File storeDir, FileSystemAbstraction fs,
            SchemaIndexProvider.Monitor monitor, Config config, OperationalMode operationalMode,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        IndexDirectoryStructure.Factory childDirectoryStructure = subProviderDirectoryStructure( storeDir );
        boolean readOnly = isReadOnly( config, operationalMode );
        NativeSchemaNumberIndexProvider nativeProvider =
                new NativeSchemaNumberIndexProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
        LuceneSchemaIndexProvider luceneProvider = LuceneSchemaIndexProviderFactory.create( fs, childDirectoryStructure, monitor, config,
                operationalMode );
        boolean useNativeIndex = config.get( GraphDatabaseSettings.enable_native_schema_index );
        int priority = useNativeIndex ? PRIORITY : 0;
        return new FusionSchemaIndexProvider( nativeProvider,
                luceneProvider, new NativeSelector(), DESCRIPTOR, priority, directoriesByProvider( storeDir ), fs );
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
