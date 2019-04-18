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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.impl.factory.OperationalMode;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

@ServiceProvider
public class GenericNativeIndexProviderFactory extends AbstractIndexProviderFactory
{
    public GenericNativeIndexProviderFactory()
    {
        super( GenericNativeIndexProvider.KEY );
    }

    @Override
    protected Class loggingClass()
    {
        return GenericNativeIndexProvider.class;
    }

    @Override
    public IndexProviderDescriptor descriptor()
    {
        return GenericNativeIndexProvider.DESCRIPTOR;
    }

    @Override
    protected GenericNativeIndexProvider internalCreate( PageCache pageCache, File storeDir, FileSystemAbstraction fs, IndexProvider.Monitor monitor,
            Config config, OperationalMode operationalMode, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        return create( pageCache, storeDir, fs, monitor, config, operationalMode, recoveryCleanupWorkCollector );
    }

    public static GenericNativeIndexProvider create( PageCache pageCache, File storeDir, FileSystemAbstraction fs, IndexProvider.Monitor monitor, Config config,
            OperationalMode mode, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        IndexDirectoryStructure.Factory directoryStructure = directoriesByProvider( storeDir );
        boolean readOnly = config.get( GraphDatabaseSettings.read_only ) && (OperationalMode.SINGLE == mode);
        return new GenericNativeIndexProvider( directoryStructure, pageCache, fs, monitor, recoveryCleanupWorkCollector, readOnly, config );
    }
}
