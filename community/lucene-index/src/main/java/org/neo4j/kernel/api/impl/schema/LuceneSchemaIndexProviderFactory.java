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

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProviderKey;

@Service.Implementation( KernelExtensionFactory.class )
public class LuceneSchemaIndexProviderFactory extends
        KernelExtensionFactory<LuceneSchemaIndexProviderFactory.Dependencies>
{
    public static final String KEY = "lucene";

    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor( KEY, "1.0" );

    public interface Dependencies
    {
        Config getConfig();

        Monitors monitors();

        LogService getLogService();

        FileSystemAbstraction fileSystem();
    }

    public LuceneSchemaIndexProviderFactory()
    {
        super( KEY );
    }

    @Override
    public LuceneSchemaIndexProvider newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        FileSystemAbstraction fileSystemAbstraction = dependencies.fileSystem();
        File storeDir = context.storeDir();
        Config config = dependencies.getConfig();
        Log log = dependencies.getLogService().getInternalLogProvider().getLog( LuceneSchemaIndexProvider.class );
        Monitors monitors = dependencies.monitors();
        monitors.addMonitorListener( new LoggingMonitor( log ), KEY );
        SchemaIndexProvider.Monitor monitor = monitors.newMonitor( SchemaIndexProvider.Monitor.class, KEY );
        OperationalMode operationalMode = context.databaseInfo().operationalMode;
        return create( fileSystemAbstraction, storeDir, monitor, config, operationalMode );
    }

    public static LuceneSchemaIndexProvider create( FileSystemAbstraction fileSystemAbstraction, File storeDir,
            SchemaIndexProvider.Monitor monitor, Config config, OperationalMode operationalMode )
    {
        return create( fileSystemAbstraction, directoriesByProviderKey( storeDir ), monitor, config, operationalMode );
    }

    public static LuceneSchemaIndexProvider create( FileSystemAbstraction fileSystemAbstraction,
            IndexDirectoryStructure.Factory directoryStructure, SchemaIndexProvider.Monitor monitor, Config config,
            OperationalMode operationalMode )
    {
        boolean ephemeral = config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral );
        DirectoryFactory directoryFactory = directoryFactory( ephemeral, fileSystemAbstraction );
        return new LuceneSchemaIndexProvider( fileSystemAbstraction, directoryFactory, directoryStructure, monitor, config,
                operationalMode );
    }
}
