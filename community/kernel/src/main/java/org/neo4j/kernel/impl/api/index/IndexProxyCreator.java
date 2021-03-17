/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.schema.IndexType.LOOKUP;

/**
 * Helper class of {@link IndexingService}. Used mainly as factory of index proxies.
 */
class IndexProxyCreator
{
    private final IndexSamplingConfig samplingConfig;
    private final IndexStatisticsStore indexStatisticsStore;
    private final IndexProviderMap providerMap;
    private final TokenNameLookup tokenNameLookup;
    private final LogProvider logProvider;

    IndexProxyCreator( IndexSamplingConfig samplingConfig,
            IndexStatisticsStore indexStatisticsStore,
            IndexProviderMap providerMap,
            TokenNameLookup tokenNameLookup,
            LogProvider logProvider )
    {
        this.samplingConfig = samplingConfig;
        this.indexStatisticsStore = indexStatisticsStore;
        this.providerMap = providerMap;
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
    }

    IndexProxy createPopulatingIndexProxy( IndexDescriptor index, boolean flipToTentative, IndexingService.Monitor monitor,
            IndexPopulationJob populationJob )
    {
        final FlippableIndexProxy flipper = new FlippableIndexProxy();

        IndexPopulator populator = populatorFromProvider( index, samplingConfig, populationJob.bufferFactory(), populationJob.getMemoryTracker() );

        IndexRepresentation indexRepresentation = createIndexProxyInfo( index );
        FailedIndexProxyFactory failureDelegateFactory = new FailedPopulatingIndexProxyFactory( indexRepresentation,
                populator,
                logProvider );

        MultipleIndexPopulator.IndexPopulation indexPopulation = populationJob
                .addPopulator( populator, indexRepresentation, flipper, failureDelegateFactory );
        PopulatingIndexProxy populatingIndex = new PopulatingIndexProxy( indexRepresentation, populationJob, indexPopulation );

        flipper.flipTo( populatingIndex );

        // Prepare for flipping to online mode
        flipper.setFlipTarget( () ->
        {
            monitor.populationCompleteOn( index );
            IndexAccessor accessor = onlineAccessorFromProvider( index, samplingConfig );
            OnlineIndexProxy onlineProxy = new OnlineIndexProxy( indexRepresentation, accessor, true );
            if ( flipToTentative )
            {
                return new TentativeConstraintIndexProxy( flipper, onlineProxy, tokenNameLookup );
            }
            return onlineProxy;
        } );

        return new ContractCheckingIndexProxy( flipper );
    }

    IndexProxy createRecoveringIndexProxy( IndexDescriptor descriptor )
    {
        IndexRepresentation indexRepresentation = createIndexProxyInfo( descriptor );
        IndexProxy proxy = new RecoveringIndexProxy( indexRepresentation );
        return new ContractCheckingIndexProxy( proxy );
    }

    IndexProxy createOnlineIndexProxy( IndexDescriptor descriptor )
    {
        try
        {
            IndexAccessor onlineAccessor = onlineAccessorFromProvider( descriptor, samplingConfig );
            IndexRepresentation indexRepresentation = createIndexProxyInfo( descriptor );
            IndexProxy proxy = new OnlineIndexProxy( indexRepresentation, onlineAccessor, false );
            // it will be started later, when recovery is completed
            return new ContractCheckingIndexProxy( proxy );
        }
        catch ( IOException e )
        {
            logProvider.getLog( getClass() ).error( "Failed to open index: " + descriptor.getId() +
                                                    " (" + descriptor.userDescription( tokenNameLookup ) +
                                                    "), requesting re-population.", e );
            return createRecoveringIndexProxy( descriptor );
        }
    }

    private IndexRepresentation createIndexProxyInfo( IndexDescriptor descriptor )
    {
        if ( descriptor.getIndexType() == LOOKUP )
        {
            boolean allowToChangeDescriptor = descriptor.getId() == IndexDescriptor.INJECTED_NLI_ID;
            return new TokenIndexRepresentation( descriptor, tokenNameLookup, allowToChangeDescriptor );
        }
        else
        {
            return new ValueIndexRepresentation( descriptor, indexStatisticsStore, tokenNameLookup );
        }
    }

    IndexProxy createFailedIndexProxy( IndexDescriptor descriptor, IndexPopulationFailure populationFailure )
    {
        // Note about the buffer factory instantiation here. Question is why an index populator is instantiated for a failed index proxy to begin with.
        // The byte buffer factory should not be used here anyway so the buffer size doesn't actually matter.
        MinimalIndexAccessor minimalIndexAccessor = minimalIndexAccessorFromProvider( descriptor );
        IndexRepresentation indexRepresentation = createIndexProxyInfo( descriptor );
        IndexProxy proxy;
        proxy = new FailedIndexProxy( indexRepresentation,
                minimalIndexAccessor,
                populationFailure,
                logProvider );
        return new ContractCheckingIndexProxy( proxy );
    }

    private IndexPopulator populatorFromProvider( IndexDescriptor index, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker )
    {
        IndexProvider provider = providerMap.lookup( index.getIndexProvider() );
        return provider.getPopulator( index, samplingConfig, bufferFactory, memoryTracker, tokenNameLookup );
    }

    private MinimalIndexAccessor minimalIndexAccessorFromProvider( IndexDescriptor index )
    {
        IndexProvider provider = providerMap.lookup( index.getIndexProvider() );
        return provider.getMinimalIndexAccessor( index );
    }

    private IndexAccessor onlineAccessorFromProvider( IndexDescriptor index, IndexSamplingConfig samplingConfig ) throws IOException
    {
        IndexProvider provider = providerMap.lookup( index.getIndexProvider() );
        return provider.getOnlineAccessor( index, samplingConfig, tokenNameLookup );
    }
}
