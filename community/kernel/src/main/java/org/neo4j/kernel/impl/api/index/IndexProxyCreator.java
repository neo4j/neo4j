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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
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

    IndexProxy createPopulatingIndexProxy( final long ruleId,
            final IndexDescriptor descriptor,
            final IndexProvider.Descriptor providerDescriptor,
            final boolean flipToTentative,
            final IndexingService.Monitor monitor,
            final IndexPopulationJob populationJob )
    {
        final FlippableIndexProxy flipper = new FlippableIndexProxy();

        final String indexUserDescription = indexUserDescription( descriptor, providerDescriptor );
        IndexPopulator populator = populatorFromProvider( providerDescriptor, ruleId, descriptor, samplingConfig );
        IndexMeta indexMeta = indexMetaFromProvider( ruleId, providerDescriptor, descriptor );

        FailedIndexProxyFactory failureDelegateFactory = new FailedPopulatingIndexProxyFactory(
                indexMeta,
                populator,
                indexUserDescription,
                new IndexCountsRemover( storeView, ruleId ),
                logProvider );

        MultipleIndexPopulator.IndexPopulation indexPopulation = populationJob
                .addPopulator( populator, ruleId, indexMeta, indexUserDescription, flipper, failureDelegateFactory );
        PopulatingIndexProxy populatingIndex = new PopulatingIndexProxy( indexMeta, populationJob, indexPopulation );

        flipper.flipTo( populatingIndex );

        // Prepare for flipping to online mode
        flipper.setFlipTarget( () ->
        {
            monitor.populationCompleteOn( descriptor );
            OnlineIndexProxy onlineProxy =
                    new OnlineIndexProxy(
                            ruleId,
                            indexMeta,
                            onlineAccessorFromProvider( providerDescriptor, ruleId, descriptor, samplingConfig ),
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

    IndexProxy createRecoveringIndexProxy( long ruleId, IndexDescriptor descriptor,
            IndexProvider.Descriptor providerDescriptor )
    {
        IndexMeta indexMeta = indexMetaFromProvider( ruleId, providerDescriptor, descriptor );
        IndexProxy proxy = new RecoveringIndexProxy( indexMeta );
        return new ContractCheckingIndexProxy( proxy, true );
    }

    IndexProxy createOnlineIndexProxy( long ruleId,
            IndexDescriptor descriptor,
            IndexProvider.Descriptor providerDescriptor )
    {
        try
        {
            IndexAccessor onlineAccessor =
                    onlineAccessorFromProvider( providerDescriptor, ruleId, descriptor, samplingConfig );
            IndexMeta indexMeta = indexMetaFromProvider( ruleId, providerDescriptor, descriptor );
            IndexProxy proxy;
            proxy = new OnlineIndexProxy( ruleId, indexMeta, onlineAccessor, storeView, false );
            proxy = new ContractCheckingIndexProxy( proxy, true );
            return proxy;
        }
        catch ( IOException e )
        {
            logProvider.getLog( getClass() ).error( "Failed to open index: " + ruleId +
                                                    " (" + descriptor.userDescription( tokenNameLookup ) +
                                                    "), requesting re-population.", e );
            return createRecoveringIndexProxy( ruleId, descriptor, providerDescriptor );
        }
    }

    IndexProxy createFailedIndexProxy( long ruleId,
            IndexDescriptor descriptor,
            IndexProvider.Descriptor providerDescriptor,
            IndexPopulationFailure populationFailure )
    {
        IndexPopulator indexPopulator = populatorFromProvider( providerDescriptor, ruleId, descriptor, samplingConfig );
        IndexMeta indexMeta = indexMetaFromProvider( ruleId, providerDescriptor, descriptor );
        String indexUserDescription = indexUserDescription( descriptor, providerDescriptor );
        IndexProxy proxy;
        proxy = new FailedIndexProxy(
                indexMeta,
                indexUserDescription,
                indexPopulator,
                populationFailure,
                new IndexCountsRemover( storeView, ruleId ),
                logProvider );
        proxy = new ContractCheckingIndexProxy( proxy, true );
        return proxy;
    }

    private String indexUserDescription( final IndexDescriptor descriptor,
                                         final IndexProvider.Descriptor providerDescriptor )
    {
        return format( "%s [provider: %s]",
                descriptor.schema().userDescription( tokenNameLookup ), providerDescriptor.toString() );
    }

    private IndexPopulator populatorFromProvider( IndexProvider.Descriptor providerDescriptor, long ruleId,
                                                  IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        IndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getPopulator( ruleId, descriptor, samplingConfig );
    }

    private IndexAccessor onlineAccessorFromProvider( IndexProvider.Descriptor providerDescriptor,
                                                      long ruleId, IndexDescriptor descriptor,
                                                      IndexSamplingConfig samplingConfig ) throws IOException
    {
        IndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getOnlineAccessor( ruleId, descriptor, samplingConfig );
    }

    private IndexMeta indexMetaFromProvider( long ruleId, IndexProvider.Descriptor providerDescriptor, IndexDescriptor indexDescriptor )
    {
        IndexCapability indexCapability = providerMap.apply( providerDescriptor ).getCapability( indexDescriptor );
        return new IndexMeta( ruleId, indexDescriptor, providerDescriptor, indexCapability );
    }
}
