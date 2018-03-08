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
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.NumberIndexProvider;
import org.neo4j.kernel.impl.index.schema.SpatialIndexProvider;
import org.neo4j.kernel.impl.index.schema.StringIndexProvider;
import org.neo4j.kernel.impl.index.schema.TemporalIndexProvider;
import org.neo4j.kernel.impl.newapi.UnionIndexCapability;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.instancesAs;

/**
 * This {@link IndexProvider index provider} act as one logical index but is backed by four physical
 * indexes, the number, spatial, temporal native indexes, and the general purpose lucene index.
 */
public class FusionIndexProvider extends IndexProvider
{
    interface Selector
    {
        int selectSlot( Value... values );

        default <T> T select( T[] instances, Value... values )
        {
            return instances[selectSlot( values )];
        }
    }

    private final IndexProvider[] providers;
    private final Selector selector;
    private final DropAction dropAction;

    public FusionIndexProvider(
            // good to be strict with specific providers here since this is dev facing
            StringIndexProvider stringProvider,
            NumberIndexProvider numberProvider,
            SpatialIndexProvider spatialProvider,
            TemporalIndexProvider temporalProvider,
            IndexProvider luceneProvider,
            Selector selector,
            Descriptor descriptor,
            int priority,
            IndexDirectoryStructure.Factory directoryStructure,
            FileSystemAbstraction fs )
    {
        super( descriptor, priority, directoryStructure );
        this.providers = array( stringProvider, numberProvider, spatialProvider, temporalProvider, luceneProvider );
        this.selector = selector;
        this.dropAction = new FileSystemDropAction( fs, directoryStructure() );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        return new FusionIndexPopulator( instancesAs( providers, IndexPopulator.class,
                provider -> provider.getPopulator( indexId, descriptor, samplingConfig ) ), selector, indexId, dropAction );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new FusionIndexAccessor(
                instancesAs( providers, IndexAccessor.class, provider -> provider.getOnlineAccessor( indexId, descriptor, samplingConfig ) ),
                selector, indexId, descriptor, dropAction );
    }

    @Override
    public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < providers.length; i++ )
        {
            writeFailure( nameOf( i ), builder, providers[i], indexId, descriptor );
        }
        String failure = builder.toString();
        if ( !failure.isEmpty() )
        {
            return failure;
        }
        throw new IllegalStateException( "None of the indexes were in a failed state" );
    }

    /**
     * @param subProviderIndex the index into the providers array to get the name of.
     * @return some name distinguishing the provider of this subProviderIndex from other providers.
     */
    private String nameOf( int subProviderIndex )
    {
        return providers[subProviderIndex].getClass().getSimpleName();
    }

    private void writeFailure( String indexName, StringBuilder builder, IndexProvider provider, long indexId, SchemaIndexDescriptor descriptor )
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
    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
    {
        InternalIndexState[] states =
                Arrays.stream( providers ).map( provider -> provider.getInitialState( indexId, descriptor ) ).toArray( InternalIndexState[]::new );
        if ( Arrays.stream( states ).anyMatch( state -> state == FAILED ) )
        {
            // One of the state is FAILED, the whole state must be considered FAILED
            return FAILED;
        }
        if ( Arrays.stream( states ).anyMatch( state -> state == POPULATING ) )
        {
            // No state is FAILED and one of the state is POPULATING, the whole state must be considered POPULATING
            return POPULATING;
        }
        // This means that all parts are ONLINE
        return InternalIndexState.ONLINE;
    }

    @Override
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        IndexCapability[] capabilities = new IndexCapability[providers.length];
        for ( int i = 0; i < providers.length; i++ )
        {
            capabilities[i] = providers[i].getCapability( schemaIndexDescriptor );
        }
        return new UnionIndexCapability( capabilities )
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
