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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.kernel.api.index.IndexDirectoryStructure.Factory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;

import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_POPULATING;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

/**
 * Base class for native indexes on top of {@link GBPTree}.
 *
 * @param <KEY> type of {@link NativeSchemaKey}
 * @param <VALUE> type of {@link NativeSchemaValue}
 */
abstract class NativeSchemaIndexProvider<KEY extends NativeSchemaKey,VALUE extends NativeSchemaValue> extends SchemaIndexProvider
{
    protected final PageCache pageCache;
    protected final FileSystemAbstraction fs;
    protected final Monitor monitor;
    protected final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    protected final boolean readOnly;

    protected NativeSchemaIndexProvider( Descriptor descriptor, int priority, Factory directoryStructureFactory, PageCache pageCache,
            FileSystemAbstraction fs, Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        super( descriptor, priority, directoryStructureFactory );
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.readOnly = readOnly;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }

        File storeFile = nativeIndexFileFromIndexId( indexId );
        switch ( descriptor.type() )
        {
        case GENERAL:
            return new NativeNonUniqueSchemaIndexPopulator<>( pageCache, fs, storeFile, layoutNonUnique(), samplingConfig,
                    monitor, descriptor, indexId );
        case UNIQUE:
            return new NativeUniqueSchemaIndexPopulator<>( pageCache, fs, storeFile, layoutUnique(), monitor, descriptor,
                    indexId );
        default:
            throw new UnsupportedOperationException( "Can not create index populator of type " + descriptor.type() );
        }
    }

    @Override
    public IndexAccessor getOnlineAccessor(
            long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        File storeFile = nativeIndexFileFromIndexId( indexId );
        Layout<KEY,VALUE> layout = layout( descriptor );
        return newIndexAccessor( storeFile, layout, descriptor, indexId, samplingConfig );
    }

    protected abstract IndexAccessor newIndexAccessor( File storeFile, Layout<KEY,VALUE> layout, IndexDescriptor descriptor,
            long indexId, IndexSamplingConfig samplingConfig ) throws IOException;

    @Override
    public String getPopulationFailure( long indexId, IndexDescriptor descriptor ) throws IllegalStateException
    {
        try
        {
            String failureMessage = readPopulationFailure( indexId, descriptor );
            if ( failureMessage == null )
            {
                throw new IllegalStateException( "Index " + indexId + " isn't failed" );
            }
            return failureMessage;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public InternalIndexState getInitialState( long indexId, IndexDescriptor descriptor )
    {
        try
        {
            NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
            GBPTree.readHeader( pageCache, nativeIndexFileFromIndexId( indexId ), layout( descriptor ), headerReader );
            switch ( headerReader.state )
            {
            case BYTE_FAILED:
                return InternalIndexState.FAILED;
            case BYTE_ONLINE:
                return InternalIndexState.ONLINE;
            case BYTE_POPULATING:
                return InternalIndexState.POPULATING;
            default:
                throw new IllegalStateException( "Unexpected initial state byte value " + headerReader.state );
            }
        }
        catch ( IOException e )
        {
            monitor.failedToOpenIndex( indexId, descriptor, "Requesting re-population.", e );
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

    private String readPopulationFailure( long indexId, IndexDescriptor descriptor ) throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, nativeIndexFileFromIndexId( indexId ), layout( descriptor ), headerReader );
        return headerReader.failureMessage;
    }

    private Layout<KEY,VALUE> layout( IndexDescriptor descriptor )
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            return layoutNonUnique();
        case UNIQUE:
            return layoutUnique();
        default:
            throw new UnsupportedOperationException( "Can not create index accessor of type " + descriptor.type() );
        }
    }

    protected abstract Layout<KEY,VALUE> layoutUnique();

    protected abstract Layout<KEY,VALUE> layoutNonUnique();

    private File nativeIndexFileFromIndexId( long indexId )
    {
        return new File( directoryStructure().directoryForIndex( indexId ), indexFileName( indexId ) );
    }

    private static String indexFileName( long indexId )
    {
        return "index-" + indexId;
    }
}
