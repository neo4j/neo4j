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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.CapableIndexDescriptor;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Helper class of {@link IndexingService}. Used mainly as factory of index proxies.
 */
class IndexProxyCreator
{
    private final IndexSamplingConfig samplingConfig;
    private final IndexStoreView storeView;
    private final IndexProviderMap providerMap;
    private final TokenNameLookup tokenNameLookup;
    private final LogProvider logProvider;

    IndexProxyCreator( IndexSamplingConfig samplingConfig,
            IndexStoreView storeView,
            IndexProviderMap providerMap,
            TokenNameLookup tokenNameLookup,
            LogProvider logProvider )
    {
        this.samplingConfig = samplingConfig;
        this.storeView = storeView;
        this.providerMap = providerMap;
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
    }

    IndexProxy createPopulatingIndexProxy( final StoreIndexDescriptor descriptor, final boolean flipToTentative, final IndexingService.Monitor monitor,
                                           final IndexPopulationJob populationJob )
    {
        final FlippableIndexProxy flipper = new FlippableIndexProxy();

        final String indexUserDescription = indexUserDescription( descriptor );
        IndexPopulator populator = populatorFromProvider( descriptor, samplingConfig );
        CapableIndexDescriptor capableIndexDescriptor = descriptor.withCapabilities( providerMap );

        FailedIndexProxyFactory failureDelegateFactory = new FailedPopulatingIndexProxyFactory( capableIndexDescriptor,
                populator,
                indexUserDescription,
                new IndexCountsRemover( storeView, descriptor.getId() ),
                logProvider );

        MultipleIndexPopulator.IndexPopulation indexPopulation = populationJob
                .addPopulator( populator, capableIndexDescriptor, indexUserDescription, flipper, failureDelegateFactory );
        PopulatingIndexProxy populatingIndex = new PopulatingIndexProxy( capableIndexDescriptor, populationJob, indexPopulation );

        flipper.flipTo( populatingIndex );

        // Prepare for flipping to online mode
        flipper.setFlipTarget( () ->
        {
            monitor.populationCompleteOn( descriptor );
            OnlineIndexProxy onlineProxy = new OnlineIndexProxy( capableIndexDescriptor, onlineAccessorFromProvider( descriptor, samplingConfig ),
                            storeView,
                            true );
            if ( flipToTentative )
            {
                return new TentativeConstraintIndexProxy( flipper, onlineProxy );
            }
            return onlineProxy;
        } );

        return new ContractCheckingIndexProxy( flipper, false );
    }

    IndexProxy createRecoveringIndexProxy( StoreIndexDescriptor descriptor )
    {
        CapableIndexDescriptor capableIndexDescriptor = descriptor.withCapabilities( providerMap );
        IndexProxy proxy = new RecoveringIndexProxy( capableIndexDescriptor );
        return new ContractCheckingIndexProxy( proxy, true );
    }

    IndexProxy createOnlineIndexProxy( StoreIndexDescriptor descriptor )
    {
        try
        {
            IndexAccessor onlineAccessor = onlineAccessorFromProvider( descriptor, samplingConfig );
            CapableIndexDescriptor capableIndexDescriptor = descriptor.withCapabilities( providerMap );
            IndexProxy proxy;
            proxy = new OnlineIndexProxy( capableIndexDescriptor, onlineAccessor, storeView, false );
            proxy = new ContractCheckingIndexProxy( proxy, true );
            return proxy;
        }
        catch ( IOException e )
        {
            logProvider.getLog( getClass() ).error( "Failed to open index: " + descriptor.getId() +
                                                    " (" + descriptor.userDescription( tokenNameLookup ) +
                                                    "), requesting re-population.", e );
            return createRecoveringIndexProxy( descriptor );
        }
    }

    IndexProxy createFailedIndexProxy( StoreIndexDescriptor descriptor, IndexPopulationFailure populationFailure )
    {
        IndexPopulator indexPopulator = populatorFromProvider( descriptor, samplingConfig );
        CapableIndexDescriptor capableIndexDescriptor = descriptor.withCapabilities( providerMap );
        String indexUserDescription = indexUserDescription( descriptor );
        IndexProxy proxy;
        proxy = new FailedIndexProxy( capableIndexDescriptor,
                indexUserDescription,
                indexPopulator,
                populationFailure,
                new IndexCountsRemover( storeView, descriptor.getId() ),
                logProvider );
        proxy = new ContractCheckingIndexProxy( proxy, true );
        return proxy;
    }

    private String indexUserDescription( final StoreIndexDescriptor descriptor )
    {
        return format( "%s [provider: %s]",
                descriptor.schema().userDescription( tokenNameLookup ), descriptor.providerDescriptor().toString() );
    }

    private IndexPopulator populatorFromProvider( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        IndexProvider indexProvider = providerMap.lookup( descriptor.providerDescriptor() );
        return indexProvider.getPopulator( descriptor, samplingConfig );
    }

    private IndexAccessor onlineAccessorFromProvider( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        IndexProvider indexProvider = providerMap.lookup( descriptor.providerDescriptor() );
        return indexProvider.getOnlineAccessor( descriptor, samplingConfig );
    }
}
