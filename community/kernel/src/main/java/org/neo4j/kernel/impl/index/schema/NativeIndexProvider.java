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
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure.Factory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;

/**
 * Base class for native indexes on top of {@link GBPTree}.
 *
 * @param <KEY> type of {@link NativeIndexKey}
 * @param <VALUE> type of {@link NativeIndexValue}
 */
abstract class NativeIndexProvider<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue> extends IndexProvider
{
    protected final PageCache pageCache;
    protected final FileSystemAbstraction fs;
    protected final Monitor monitor;
    protected final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    protected final boolean readOnly;

    protected NativeIndexProvider( Descriptor descriptor, int priority, Factory directoryStructureFactory, PageCache pageCache, FileSystemAbstraction fs,
            Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        super( descriptor, priority, directoryStructureFactory );
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.readOnly = readOnly;
    }

    abstract Layout<KEY,VALUE> layout( StoreIndexDescriptor descriptor );

    @Override
    public IndexPopulator getPopulator( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }

        File storeFile = nativeIndexFileFromIndexId( descriptor.getId() );
        return newIndexPopulator( storeFile, layout( descriptor ), descriptor, samplingConfig );
    }

    protected abstract IndexPopulator newIndexPopulator( File storeFile, Layout<KEY,VALUE> layout, StoreIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig );

    @Override
    public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        File storeFile = nativeIndexFileFromIndexId( descriptor.getId() );
        return newIndexAccessor( storeFile, layout( descriptor ), descriptor, samplingConfig );
    }

    protected abstract IndexAccessor newIndexAccessor( File storeFile, Layout<KEY,VALUE> layout, StoreIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException;

    @Override
    public String getPopulationFailure( StoreIndexDescriptor descriptor ) throws IllegalStateException
    {
        try
        {
            String failureMessage = NativeIndexes.readFailureMessage( pageCache, nativeIndexFileFromIndexId( descriptor.getId() ) );
            if ( failureMessage == null )
            {
                throw new IllegalStateException( "Index " + descriptor.getId() + " isn't failed" );
            }
            return failureMessage;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public InternalIndexState getInitialState( StoreIndexDescriptor descriptor )
    {
        try
        {
            return NativeIndexes.readState( pageCache, nativeIndexFileFromIndexId( descriptor.getId() ) );
        }
        catch ( MetadataMismatchException | IOException e )
        {
            monitor.failedToOpenIndex( descriptor, "Requesting re-population.", e );
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        // Since this native provider is a new one, there's no need for migration on this level.
        // Migration should happen in the combined layer for the time being.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    private File nativeIndexFileFromIndexId( long indexId )
    {
        return new File( directoryStructure().directoryForIndex( indexId ), indexFileName( indexId ) );
    }

    private static String indexFileName( long indexId )
    {
        return "index-" + indexId;
    }
}
