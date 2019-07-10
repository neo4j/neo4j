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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.eclipse.collections.api.tuple.Pair;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.ByteBufferFactory;
import org.neo4j.kernel.impl.storemigration.SchemaIndexMigrator;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.values.storable.Value;

import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.GENERIC;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;

/**
 * This {@link IndexProvider index provider} act as one logical index but is backed by four physical
 * indexes, the number, spatial, temporal native indexes, and the general purpose lucene index.
 */
public class FusionIndexProvider extends IndexProvider
{
    private final boolean archiveFailedIndex;
    private final InstanceSelector<IndexProvider> providers;
    private final SlotSelector slotSelector;
    private final FileSystemAbstraction fs;

    public FusionIndexProvider(
            // good to be strict with specific providers here since this is dev facing
            IndexProvider genericProvider,
            IndexProvider luceneProvider,
            SlotSelector slotSelector,
            IndexProviderDescriptor descriptor,
            IndexDirectoryStructure.Factory directoryStructure,
            FileSystemAbstraction fs,
            boolean archiveFailedIndex )
    {
        super( descriptor, directoryStructure );
        this.archiveFailedIndex = archiveFailedIndex;
        this.slotSelector = slotSelector;
        this.providers = new InstanceSelector<>();
        this.fs = fs;
        fillProvidersSelector( genericProvider, luceneProvider );
        slotSelector.validateSatisfied( providers );
    }

    private void fillProvidersSelector( IndexProvider genericProvider,
            IndexProvider luceneProvider )
    {
        providers.put( GENERIC, genericProvider );
        providers.put( LUCENE, luceneProvider );
    }

    @Override
    public IndexDescriptor completeConfiguration( IndexDescriptor index )
    {
        EnumMap<IndexSlot,IndexDescriptor> descriptors = new EnumMap<>( IndexSlot.class );
        EnumMap<IndexSlot,IndexCapability> capabilities = new EnumMap<>( IndexSlot.class );
        for ( IndexSlot slot : IndexSlot.values() )
        {
            IndexDescriptor result = providers.select( slot ).completeConfiguration( index );
            descriptors.put( slot, result );
            capabilities.put( slot, result.getCapability() );
        }
        IndexConfig config = index.schema().getIndexConfig();
        for ( IndexDescriptor result : descriptors.values() )
        {
            IndexConfig resultConfig = result.schema().getIndexConfig();
            for ( Pair<String,Value> entry : resultConfig.entries() )
            {
                config = config.withIfAbsent( entry.getOne(), entry.getTwo() );
            }
        }
        index = index.withSchemaDescriptor( index.schema().withIndexConfig( config ) );
        if ( index.getCapability().equals( IndexCapability.NO_CAPABILITY ) )
        {
            index = index.withIndexCapability( new FusionIndexCapability( slotSelector, new InstanceSelector<>( capabilities ) ) );
        }
        return index;
    }

    @Override
    public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory )
    {
        EnumMap<IndexSlot,IndexPopulator> populators = providers.map( provider -> provider.getPopulator( descriptor, samplingConfig, bufferFactory ) );
        return new FusionIndexPopulator( slotSelector, new InstanceSelector<>( populators ), descriptor.getId(), fs, directoryStructure(),
                archiveFailedIndex );
    }

    @Override
    public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        EnumMap<IndexSlot,IndexAccessor> accessors = providers.map( provider -> provider.getOnlineAccessor( descriptor, samplingConfig ) );
        return new FusionIndexAccessor( slotSelector, new InstanceSelector<>( accessors ), descriptor, fs, directoryStructure() );
    }

    @Override
    public String getPopulationFailure( IndexDescriptor descriptor ) throws IllegalStateException
    {
        StringBuilder builder = new StringBuilder();
        providers.forAll( p -> writeFailure( p.getClass().getSimpleName(), builder, p, descriptor ) );
        String failure = builder.toString();
        if ( !failure.isEmpty() )
        {
            return failure;
        }
        throw new IllegalStateException( "None of the indexes were in a failed state" );
    }

    private void writeFailure( String indexName, StringBuilder builder, IndexProvider provider, IndexDescriptor descriptor )
    {
        try
        {
            String failure = provider.getPopulationFailure( descriptor );
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
    public InternalIndexState getInitialState( IndexDescriptor descriptor )
    {
        Iterable<InternalIndexState> statesIterable = providers.transform( p -> p.getInitialState( descriptor ) );
        List<InternalIndexState> states = Iterables.asList( statesIterable );
        if ( states.contains( FAILED ) )
        {
            // One of the state is FAILED, the whole state must be considered FAILED
            return FAILED;
        }
        if ( states.contains( POPULATING ) )
        {
            // No state is FAILED and one of the state is POPULATING, the whole state must be considered POPULATING
            return POPULATING;
        }
        // This means that all parts are ONLINE
        return InternalIndexState.ONLINE;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache, StorageEngineFactory storageEngineFactory )
    {
        return new SchemaIndexMigrator( fs, this, storageEngineFactory );
    }
}
