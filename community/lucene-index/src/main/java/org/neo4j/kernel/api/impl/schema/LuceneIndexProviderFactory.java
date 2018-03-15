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

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSelector00;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;
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
        FileSystemAbstraction fileSystemAbstraction = dependencies.fileSystem();
        File storeDir = context.storeDir();
        Config config = dependencies.getConfig();
        Log log = dependencies.getLogService().getInternalLogProvider().getLog( LuceneIndexProvider.class );
        Monitors monitors = dependencies.monitors();
        monitors.addMonitorListener( new LoggingMonitor( log ), KEY );
        IndexProvider.Monitor monitor = monitors.newMonitor( IndexProvider.Monitor.class, KEY );
        OperationalMode operationalMode = context.databaseInfo().operationalMode;

        LuceneIndexProvider luceneProvider = createLuceneProvider( fileSystemAbstraction, storeDir, monitor, config, operationalMode );

        return new FusionIndexProvider( EMPTY, EMPTY, EMPTY, EMPTY, luceneProvider, new FusionSelector00(),
                PROVIDER_DESCRIPTOR, LuceneIndexProvider.PRIORITY, directoriesByProvider( storeDir ), fileSystemAbstraction );
    }

    public static LuceneIndexProvider createLuceneProvider( FileSystemAbstraction fileSystemAbstraction, File storeDir,
                                              IndexProvider.Monitor monitor, Config config, OperationalMode operationalMode )
    {
        return createLuceneProvider( fileSystemAbstraction, directoryStructureForLuceneProvider( storeDir ), monitor, config, operationalMode );
    }

    static LuceneIndexProvider createLuceneProvider( FileSystemAbstraction fileSystemAbstraction,
            IndexDirectoryStructure.Factory directoryStructure, IndexProvider.Monitor monitor, Config config,
            OperationalMode operationalMode )
    {
        boolean ephemeral = config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral );
        DirectoryFactory directoryFactory = directoryFactory( ephemeral, fileSystemAbstraction );
        return new LuceneIndexProvider( fileSystemAbstraction, directoryFactory, directoryStructure, monitor, config,
                operationalMode );
    }

    private static IndexDirectoryStructure.Factory directoryStructureForLuceneProvider( File storeDir )
    {
        return directoriesByProviderKey( storeDir );
    }
}
