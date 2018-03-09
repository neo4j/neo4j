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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.newapi.UnionIndexCapability;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;

/**
 * This {@link IndexProvider index provider} act as one logical index but is backed by four physical
 * indexes, the number, spatial, temporal native indexes, and the general purpose lucene index.
 */
public class FusionIndexProvider extends IndexProvider
{
    interface Selector
    {
        <T> T select( T numberInstance, T spatialInstance, T temporalInstance, T luceneInstance, Value... values );
    }

    private final IndexProvider numberProvider;
    private final IndexProvider spatialProvider;
    private final IndexProvider temporalProvider;
    private final IndexProvider luceneProvider;
    private final Selector selector;
    private final DropAction dropAction;

    public FusionIndexProvider( IndexProvider numberProvider,
                                IndexProvider spatialProvider,
                                IndexProvider temporalProvider,
                                IndexProvider luceneProvider,
                                Selector selector,
                                Descriptor descriptor,
                                int priority,
                                IndexDirectoryStructure.Factory directoryStructure,
                                FileSystemAbstraction fs )
    {
        super( descriptor, priority, directoryStructure );
        this.numberProvider = numberProvider;
        this.spatialProvider = spatialProvider;
        this.temporalProvider = temporalProvider;
        this.luceneProvider = luceneProvider;
        this.selector = selector;
        this.dropAction = new FileSystemDropAction( fs, directoryStructure() );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        return new FusionIndexPopulator(
                numberProvider.getPopulator( indexId, descriptor, samplingConfig ),
                spatialProvider.getPopulator( indexId, descriptor, samplingConfig ),
                temporalProvider.getPopulator( indexId, descriptor, samplingConfig ),
                luceneProvider.getPopulator( indexId, descriptor, samplingConfig ), selector, indexId, dropAction );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new FusionIndexAccessor(
                numberProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ),
                spatialProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ),
                temporalProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ),
                luceneProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ), selector, indexId, descriptor, dropAction );
    }

    @Override
    public String getPopulationFailure( long indexId, IndexDescriptor descriptor ) throws IllegalStateException
    {
        StringBuilder builder = new StringBuilder();
        writeFailure( "number", builder, numberProvider, indexId, descriptor );
        writeFailure( "spatial", builder, spatialProvider, indexId, descriptor );
        writeFailure( "temporal", builder, temporalProvider, indexId, descriptor );
        writeFailure( "lucene", builder, luceneProvider, indexId, descriptor );
        String failure = builder.toString();
        if ( !failure.isEmpty() )
        {
            return failure;
        }
        throw new IllegalStateException( "None of the indexes were in a failed state" );
    }

    private void writeFailure( String indexName, StringBuilder builder, IndexProvider provider, long indexId, IndexDescriptor descriptor )
    {
        try
        {
            String failure = provider.getPopulationFailure( indexId, descriptor );
            builder.append( indexName );
            builder.append( ": " );
            builder.append( failure );
            builder.append( ' ' );
        }
        catch ( IllegalStateException e )
        {   // Just catch
        }
    }

    @Override
    public InternalIndexState getInitialState( long indexId, IndexDescriptor descriptor )
    {
        InternalIndexState numberState = numberProvider.getInitialState( indexId, descriptor );
        InternalIndexState spatialState = spatialProvider.getInitialState( indexId, descriptor );
        InternalIndexState temporalState = temporalProvider.getInitialState( indexId, descriptor );
        InternalIndexState luceneState = luceneProvider.getInitialState( indexId, descriptor );
        if ( numberState == FAILED || spatialState == FAILED  || temporalState == FAILED || luceneState == FAILED )
        {
            // One of the state is FAILED, the whole state must be considered FAILED
            return FAILED;
        }
        if ( numberState == POPULATING || spatialState == POPULATING || temporalState == POPULATING || luceneState == POPULATING )
        {
            // No state is FAILED and one of the state is POPULATING, the whole state must be considered POPULATING
            return POPULATING;
        }
        // This means that all parts are ONLINE
        return InternalIndexState.ONLINE;
    }

    @Override
    public IndexCapability getCapability( IndexDescriptor indexDescriptor )
    {
        IndexCapability numberCapability = numberProvider.getCapability( indexDescriptor );
        IndexCapability spatialCapability = spatialProvider.getCapability( indexDescriptor );
        IndexCapability temporalCapability = temporalProvider.getCapability( indexDescriptor );
        IndexCapability luceneCapability = luceneProvider.getCapability( indexDescriptor );
        return new UnionIndexCapability( numberCapability, spatialCapability, temporalCapability, luceneCapability )
        {
            @Override
            public IndexOrder[] orderCapability( ValueGroup... valueGroups )
            {
                // No order capability when combining results from different indexes
                if ( valueGroups.length == 1 && valueGroups[0] == ValueGroup.UNKNOWN )
                {
                    return new IndexOrder[0];
                }
                // Otherwise union of capabilities
                return super.orderCapability( valueGroups );
            }
        };
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        // TODO implementation of this depends on decisions around defaults and migration. Coming soon.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    static IndexSample combineSamples( IndexSample... samples )
    {
        long indexSize = Arrays.stream(samples).mapToLong( IndexSample::indexSize ).sum();
        long uniqueValues = Arrays.stream(samples).mapToLong( IndexSample::uniqueValues ).sum();
        long sampleSize = Arrays.stream(samples).mapToLong( IndexSample::sampleSize ).sum();
        return new IndexSample( indexSize, uniqueValues, sampleSize );
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
