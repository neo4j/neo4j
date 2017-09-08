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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;

/**
 * This {@link SchemaIndexProvider index provider} act as one logical index but is backed by two physical
 * indexes, the native index and the lucene index. All index entries that can be handled by the native index will be directed
 * there and the rest will be directed to the lucene index.
 */
public class FusionSchemaIndexProvider extends SchemaIndexProvider
{
    public interface Selector
    {
        <T> T select( T nativeInstance, T luceneInstance, Value... values );
    }

    private final SchemaIndexProvider nativeProvider;
    private final SchemaIndexProvider luceneProvider;
    private final Selector selector;
    private final DropAction dropAction;

    public FusionSchemaIndexProvider( SchemaIndexProvider nativeProvider,
            SchemaIndexProvider luceneProvider, Selector selector, SchemaIndexProvider.Descriptor descriptor,
            int priority, IndexDirectoryStructure.Factory directoryStructure, FileSystemAbstraction fs )
    {
        super( descriptor, priority, directoryStructure );
        this.nativeProvider = nativeProvider;
        this.luceneProvider = luceneProvider;
        this.selector = selector;
        this.dropAction = new FileSystemDropAction( fs, directoryStructure() );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        return new FusionIndexPopulator(
                nativeProvider.getPopulator( indexId, descriptor, samplingConfig ),
                luceneProvider.getPopulator( indexId, descriptor, samplingConfig ), selector, indexId, dropAction );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new FusionIndexAccessor(
                nativeProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ),
                luceneProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ), selector, indexId, dropAction );
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        String nativeFailure = null;
        try
        {
            nativeFailure = nativeProvider.getPopulationFailure( indexId );
        }
        catch ( IllegalStateException e )
        {   // Just catch
        }
        String luceneFailure = null;
        try
        {
            luceneFailure = luceneProvider.getPopulationFailure( indexId );
        }
        catch ( IllegalStateException e )
        {   // Just catch
        }

        if ( nativeFailure != null || luceneFailure != null )
        {
            return "native: " + nativeFailure + " lucene: " + luceneFailure;
        }
        throw new IllegalStateException( "None of the indexes were in a failed state" );
    }

    @Override
    public InternalIndexState getInitialState( long indexId, IndexDescriptor descriptor )
    {
        InternalIndexState nativeState = nativeProvider.getInitialState( indexId, descriptor );
        InternalIndexState luceneState = luceneProvider.getInitialState( indexId, descriptor );
        if ( nativeState == InternalIndexState.FAILED || luceneState == InternalIndexState.FAILED )
        {
            // One of the state is FAILED, the whole state must be considered FAILED
            return InternalIndexState.FAILED;
        }
        if ( nativeState == InternalIndexState.POPULATING || luceneState == InternalIndexState.POPULATING )
        {
            // No state is FAILED and one of the state is POPULATING, the whole state must be considered POPULATING
            return InternalIndexState.POPULATING;
        }
        // This means that both states are ONLINE
        return nativeState;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        // TODO implementation of this depends on decisions around defaults and migration. Coming soon.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    static IndexSample combineSamples( IndexSample first, IndexSample other )
    {
        return new IndexSample(
                first.indexSize() + other.indexSize(),
                first.uniqueValues() + other.uniqueValues(),
                first.sampleSize() + other.sampleSize() );
    }

    /**
     * As an interface because this is actually dependent on whether or not an index lives on a {@link FileSystemAbstraction}
     * or a page cache. At the time of writing this there's only the possibility to put these on the file system,
     * but there will be a possibility to put these in the page cache file management instead and having this abstracted
     * will help when making that switch/decision.
     */
    @FunctionalInterface
    interface DropAction
    {
        /**
         * Deletes the index directory and everything in it, as last part of dropping an index.
         *
         * @param indexId the index id, for which directory to drop.
         * @throws IOException on I/O error.
         */
        void drop( long indexId ) throws IOException;
    }

    private static class FileSystemDropAction implements DropAction
    {
        private final FileSystemAbstraction fs;
        private final IndexDirectoryStructure directoryStructure;

        FileSystemDropAction( FileSystemAbstraction fs, IndexDirectoryStructure directoryStructure )
        {
            this.fs = fs;
            this.directoryStructure = directoryStructure;
        }

        @Override
        public void drop( long indexId ) throws IOException
        {
            fs.deleteRecursively( directoryStructure.directoryForIndex( indexId ) );
        }
    }
}
