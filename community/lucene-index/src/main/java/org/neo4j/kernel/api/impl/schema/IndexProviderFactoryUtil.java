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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.NumberIndexProvider;
import org.neo4j.kernel.impl.index.schema.SpatialIndexProvider;
import org.neo4j.kernel.impl.index.schema.StringIndexProvider;
import org.neo4j.kernel.impl.index.schema.TemporalIndexProvider;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;

class IndexProviderFactoryUtil
{
    static boolean isReadOnly( Config config, OperationalMode operationalMode )
    {
        return config.get( GraphDatabaseSettings.read_only ) && (OperationalMode.single == operationalMode);
    }

    static StringIndexProvider stringProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory childDirectoryStructure,
            IndexProvider.Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        return new StringIndexProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
    }

    static NumberIndexProvider numberProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory childDirectoryStructure,
            IndexProvider.Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        return new NumberIndexProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
    }

    static SpatialIndexProvider spatialProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory directoryStructure,
            IndexProvider.Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly, Config config )
    {
        return new SpatialIndexProvider( pageCache, fs, directoryStructure, monitor, recoveryCleanupWorkCollector, readOnly, config );
    }

    static TemporalIndexProvider temporalProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory directoryStructure,
            IndexProvider.Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        return new TemporalIndexProvider( pageCache, fs, directoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
    }

    static LuceneIndexProvider luceneProvider( FileSystemAbstraction fs, IndexDirectoryStructure.Factory directoryStructure, IndexProvider.Monitor monitor,
            Config config, OperationalMode operationalMode )
    {
        boolean ephemeral = config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral );
        DirectoryFactory directoryFactory = directoryFactory( ephemeral, fs );
        return new LuceneIndexProvider( fs, directoryFactory, directoryStructure, monitor, config, operationalMode );
    }
}
