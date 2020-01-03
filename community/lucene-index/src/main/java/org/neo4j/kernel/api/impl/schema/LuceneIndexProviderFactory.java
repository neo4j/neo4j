/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.SpatialIndexProvider;
import org.neo4j.kernel.impl.index.schema.TemporalIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSlotSelector00;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.LUCENE10;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProviderKey;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;

@Service.Implementation( KernelExtensionFactory.class )
public class LuceneIndexProviderFactory extends AbstractIndexProviderFactory<LuceneIndexProviderFactory.Dependencies>
{
    private static final String KEY = LUCENE10.providerKey();
    public static final IndexProviderDescriptor PROVIDER_DESCRIPTOR = new IndexProviderDescriptor( KEY, LUCENE10.providerVersion() );

    public LuceneIndexProviderFactory()
    {
        super( KEY );
    }

    @Override
    protected Class loggingClass()
    {
        return LuceneIndexProvider.class;
    }

    @Override
    protected String descriptorString()
    {
        return PROVIDER_DESCRIPTOR.toString();
    }

    @Override
    protected IndexProvider internalCreate( PageCache pageCache, File storeDir, FileSystemAbstraction fs, IndexProvider.Monitor monitor, Config config,
            OperationalMode operationalMode, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        return newInstance( pageCache, storeDir, fs, monitor, config, operationalMode, recoveryCleanupWorkCollector );
    }

    public static FusionIndexProvider newInstance( PageCache pageCache, File databaseDirectory, FileSystemAbstraction fs,
            IndexProvider.Monitor monitor, Config config, OperationalMode operationalMode,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        boolean readOnly = IndexProviderFactoryUtil.isReadOnly( config, operationalMode );
        boolean archiveFailedIndex = config.get( GraphDatabaseSettings.archive_failed_index );
        IndexDirectoryStructure.Factory luceneDirStructure = directoriesByProviderKey( databaseDirectory );
        IndexDirectoryStructure.Factory childDirectoryStructure = subProviderDirectoryStructure( databaseDirectory );

        LuceneIndexProvider lucene = IndexProviderFactoryUtil.luceneProvider( fs, luceneDirStructure, monitor, config, operationalMode );
        TemporalIndexProvider temporal =
                IndexProviderFactoryUtil.temporalProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly );
        SpatialIndexProvider spatial =
                IndexProviderFactoryUtil.spatialProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly, config );

        return new FusionIndexProvider( EMPTY, EMPTY, spatial, temporal, lucene, new FusionSlotSelector00(),
                PROVIDER_DESCRIPTOR, directoriesByProvider( databaseDirectory ), fs, archiveFailedIndex );
    }

    private static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File databaseDirectory )
    {
        return NativeLuceneFusionIndexProviderFactory.subProviderDirectoryStructure( databaseDirectory, PROVIDER_DESCRIPTOR );
    }

    public interface Dependencies extends AbstractIndexProviderFactory.Dependencies
    {
    }
}
