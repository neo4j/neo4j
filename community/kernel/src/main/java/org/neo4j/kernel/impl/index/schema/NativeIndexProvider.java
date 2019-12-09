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

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure.Factory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

/**
 * Base class for native indexes on top of {@link GBPTree}.
 *
 * @param <KEY> type of {@link NativeIndexKey}
 * @param <VALUE> type of {@link NativeIndexValue}
 * @param <LAYOUT> type of {@link IndexLayout}
 */
abstract class NativeIndexProvider<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue,LAYOUT extends IndexLayout<KEY,VALUE>>
        extends IndexProvider
{
    protected final DatabaseIndexContext databaseIndexContext;
    protected final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;

    protected NativeIndexProvider( DatabaseIndexContext databaseIndexContext, IndexProviderDescriptor descriptor,
            Factory directoryStructureFactory, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        super( descriptor, directoryStructureFactory );
        this.databaseIndexContext = databaseIndexContext;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
    }

    /**
     * Instantiates the {@link Layout} which is used in the index backing this native index provider.
     *
     * @param descriptor the {@link IndexDescriptor} for this index.
     * @param storeFile index store file, since some layouts may depend on contents of the header.
     * If {@code null} it means that nothing must be read from the file before or while instantiating the layout.
     * @return the correct {@link Layout} for the index.
     */
    abstract LAYOUT layout( IndexDescriptor descriptor, File storeFile );

    @Override
    public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory )
    {
        if ( databaseIndexContext.readOnly )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }

        IndexFiles indexFiles = new IndexFiles.Directory( databaseIndexContext.fileSystem, directoryStructure(), descriptor.getId() );
        return newIndexPopulator( indexFiles, layout( descriptor, null /*meaning don't read from this file since we're recreating it anyway*/ ), descriptor,
                bufferFactory );
    }

    protected abstract IndexPopulator newIndexPopulator( IndexFiles indexFiles, LAYOUT layout, IndexDescriptor descriptor,
            ByteBufferFactory bufferFactory );

    @Override
    public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        IndexFiles indexFiles = new IndexFiles.Directory( databaseIndexContext.fileSystem, directoryStructure(), descriptor.getId() );
        return newIndexAccessor( indexFiles, layout( descriptor, indexFiles.getStoreFile() ), descriptor );
    }

    protected abstract IndexAccessor newIndexAccessor( IndexFiles indexFiles, LAYOUT layout, IndexDescriptor descriptor ) throws IOException;

    @Override
    public String getPopulationFailure( IndexDescriptor descriptor )
    {
        try
        {
            String failureMessage = NativeIndexes.readFailureMessage( databaseIndexContext.pageCache, storeFile( descriptor ) );
            return defaultIfEmpty( failureMessage, StringUtils.EMPTY );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public InternalIndexState getInitialState( IndexDescriptor descriptor )
    {
        try
        {
            return NativeIndexes.readState( databaseIndexContext.pageCache, storeFile( descriptor ) );
        }
        catch ( MetadataMismatchException | IOException e )
        {
            databaseIndexContext.monitor.failedToOpenIndex( descriptor, "Requesting re-population.", e );
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache, StorageEngineFactory storageEngineFactory )
    {
        // Since this native provider is a new one, there's no need for migration on this level.
        // Migration should happen in the combined layer for the time being.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    private File storeFile( IndexDescriptor descriptor )
    {
        IndexFiles indexFiles = new IndexFiles.Directory( databaseIndexContext.fileSystem, directoryStructure(), descriptor.getId() );
        return indexFiles.getStoreFile();
    }
}
