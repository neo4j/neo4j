/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider.DESCRIPTOR;

@Service.Implementation( KernelExtensionFactory.class )
public class GenericNativeIndexProviderFactory extends KernelExtensionFactory<GenericNativeIndexProviderFactory.Dependencies>
{
    private static final int PRIORITY = 0; // TODO: Zero because should not be default yet.

    public GenericNativeIndexProviderFactory()
    {
        super( ExtensionType.DATABASE, GenericNativeIndexProvider.KEY );
    }

    public interface Dependencies
    {
        PageCache pageCache();

        FileSystemAbstraction fileSystem();

        LogService getLogService();

        Monitors monitors();

        Config getConfig();

        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector();
    }

    @Override
    public GenericNativeIndexProvider newInstance( KernelContext context, Dependencies dependencies )
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

    public static GenericNativeIndexProvider create( PageCache pageCache, File storeDir, FileSystemAbstraction fs, IndexProvider.Monitor monitor, Config config,
            OperationalMode mode, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        String selectedSchemaProvider = config.get( GraphDatabaseSettings.default_schema_provider );
        int priority = PRIORITY;
        if ( GraphDatabaseSettings.SchemaIndex.NATIVE_GBPTREE10.providerName().equals( selectedSchemaProvider ) )
        {
            priority = 100;
        }

        IndexDirectoryStructure.Factory directoryStructure = directoriesByProvider( storeDir );
        boolean readOnly = config.get( GraphDatabaseSettings.read_only ) && (OperationalMode.single == mode);
        return new GenericNativeIndexProvider( priority, directoryStructure, pageCache, fs, monitor, recoveryCleanupWorkCollector, readOnly );
    }
}
