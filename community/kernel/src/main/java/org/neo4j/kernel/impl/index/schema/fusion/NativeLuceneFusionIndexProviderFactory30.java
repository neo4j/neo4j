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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.nio.file.Path;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.schema.IndexProviderFactoryUtil;
import org.neo4j.kernel.api.impl.schema.LuceneIndexProvider;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.DatabaseIndexContext;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

@ServiceProvider
public class NativeLuceneFusionIndexProviderFactory30 extends AbstractIndexProviderFactory
{
    public static final String KEY = NATIVE30.providerKey();
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( KEY, NATIVE30.providerVersion() );

    public NativeLuceneFusionIndexProviderFactory30()
    {
        super( KEY );
    }

    @Override
    protected Class<?> loggingClass()
    {
        return FusionIndexProvider.class;
    }

    @Override
    public IndexProviderDescriptor descriptor()
    {
        return DESCRIPTOR;
    }

    @Override
    protected IndexProvider internalCreate( PageCache pageCache, Path storeDir, FileSystemAbstraction fs, Monitors monitors, String monitorTag,
            Config config, OperationalMode operationalMode, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        return create( pageCache, storeDir, fs, monitors, monitorTag, config, operationalMode, recoveryCleanupWorkCollector );
    }

    @VisibleForTesting
    public static FusionIndexProvider create( PageCache pageCache, Path databaseDirectory, FileSystemAbstraction fs,
            Monitors monitors, String monitorTag, Config config, OperationalMode operationalMode,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        IndexDirectoryStructure.Factory childDirectoryStructure = subProviderDirectoryStructure( databaseDirectory );
        boolean isSingleInstance = operationalMode == OperationalMode.SINGLE;
        boolean readOnly = IndexProviderFactoryUtil.isReadOnly( config, isSingleInstance );
        boolean archiveFailedIndex = config.get( GraphDatabaseInternalSettings.archive_failed_index );

        DatabaseIndexContext databaseIndexContext =
                DatabaseIndexContext.builder( pageCache, fs ).withMonitors( monitors ).withTag( monitorTag ).withReadOnly( readOnly ).build();
        GenericNativeIndexProvider generic =
                new GenericNativeIndexProvider( databaseIndexContext, childDirectoryStructure,
                        recoveryCleanupWorkCollector, config );
        LuceneIndexProvider lucene = IndexProviderFactoryUtil.luceneProvider( fs, childDirectoryStructure, monitors, monitorTag, config, isSingleInstance );

        return new FusionIndexProvider( generic, lucene, new FusionSlotSelector30(),
                DESCRIPTOR, directoriesByProvider( databaseDirectory ), fs, archiveFailedIndex );
    }

    @VisibleForTesting
    public static IndexDirectoryStructure.Factory subProviderDirectoryStructure( Path databaseDirectory )
    {
        return subProviderDirectoryStructure( databaseDirectory, DESCRIPTOR );
    }

    @VisibleForTesting
    public static IndexDirectoryStructure.Factory subProviderDirectoryStructure( Path databaseDirectory, IndexProviderDescriptor descriptor )
    {
        IndexDirectoryStructure parentDirectoryStructure = directoriesByProvider( databaseDirectory ).forProvider( descriptor );
        return directoriesBySubProvider( parentDirectoryStructure );
    }
}
