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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Helper class of {@link IndexingService}. Used mainly as factory of index proxies.
 */
public class IndexProxyCreator
{
    private final IndexSamplingConfig samplingConfig;
    private final IndexStoreView storeView;
    private final SchemaIndexProviderMap providerMap;
    private final TokenNameLookup tokenNameLookup;
    private final LogProvider logProvider;

    public IndexProxyCreator( IndexSamplingConfig samplingConfig,
                            IndexStoreView storeView,
                            SchemaIndexProviderMap providerMap,
                            TokenNameLookup tokenNameLookup,
                            LogProvider logProvider )
    {
        this.samplingConfig = samplingConfig;
        this.storeView = storeView;
        this.providerMap = providerMap;
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
    }

    public IndexProxy createPopulatingIndexProxy( final long ruleId,
                                                  final IndexDescriptor descriptor,
                                                  final SchemaIndexProvider.Descriptor providerDescriptor,
                                                  final boolean flipToTentative,
                                                  final IndexingService.Monitor monitor,
                                                  final IndexPopulationJob populationJob ) throws IOException
    {
        final FlippableIndexProxy flipper = new FlippableIndexProxy();

        // TODO: This is here because there is a circular dependency from PopulatingIndexProxy to FlippableIndexProxy
        final String indexUserDescription = indexUserDescription( descriptor, providerDescriptor );
        IndexPopulator populator = populatorFromProvider( providerDescriptor, ruleId, descriptor, samplingConfig );

        FailedIndexProxyFactory failureDelegateFactory = new FailedPopulatingIndexProxyFactory(
                descriptor,
                providerDescriptor,
                populator,
                indexUserDescription,
                new IndexCountsRemover( storeView, ruleId ),
                logProvider
        );

        PopulatingIndexProxy populatingIndex =
                new PopulatingIndexProxy( descriptor, providerDescriptor, populationJob );

        populationJob.addPopulator( populator, ruleId, descriptor, providerDescriptor, indexUserDescription,
                flipper, failureDelegateFactory );

        flipper.flipTo( populatingIndex );

        // Prepare for flipping to online mode
        flipper.setFlipTarget( () ->
        {
            monitor.populationCompleteOn( descriptor );
            OnlineIndexProxy onlineProxy =
                    new OnlineIndexProxy(
                            ruleId,
                            descriptor,
                            onlineAccessorFromProvider( providerDescriptor, ruleId, descriptor, samplingConfig ),
                            storeView,
                            providerDescriptor, true );
            if ( flipToTentative )
            {
                return new TentativeConstraintIndexProxy( flipper, onlineProxy );
            }
            return onlineProxy;
        } );

        return new ContractCheckingIndexProxy( flipper, false );
    }

    public IndexProxy createRecoveringIndexProxy( IndexDescriptor descriptor,
                                                  SchemaIndexProvider.Descriptor providerDescriptor )
    {
        IndexProxy proxy = new RecoveringIndexProxy( descriptor, providerDescriptor );
        return new ContractCheckingIndexProxy( proxy, true );
    }

    public IndexProxy createOnlineIndexProxy( long ruleId,
                                              IndexDescriptor descriptor,
                                              SchemaIndexProvider.Descriptor providerDescriptor )
    {
        // TODO Hook in version verification/migration calls to the SchemaIndexProvider here
        try
        {
            IndexAccessor onlineAccessor =
                    onlineAccessorFromProvider( providerDescriptor, ruleId, descriptor, samplingConfig );
            IndexProxy proxy;
            proxy = new OnlineIndexProxy( ruleId, descriptor, onlineAccessor, storeView, providerDescriptor, false );
            proxy = new ContractCheckingIndexProxy( proxy, true );
            return proxy;
        }
        catch ( IOException e )
        {
            logProvider.getLog( getClass() ).error( "Failed to open index: " + ruleId +
                                                    " (" + descriptor.userDescription( tokenNameLookup ) +
                                                    "), requesting re-population.", e );
            return createRecoveringIndexProxy( descriptor, providerDescriptor );
        }
    }

    public IndexProxy createFailedIndexProxy( long ruleId,
                                              IndexDescriptor descriptor,
                                              SchemaIndexProvider.Descriptor providerDescriptor,
                                              IndexPopulationFailure populationFailure )
    {
        IndexPopulator indexPopulator =
                populatorFromProvider( providerDescriptor, ruleId, descriptor, samplingConfig );
        String indexUserDescription = indexUserDescription( descriptor, providerDescriptor );
        IndexProxy proxy;
        proxy = new FailedIndexProxy(
                descriptor,
                providerDescriptor,
                indexUserDescription,
                indexPopulator,
                populationFailure,
                new IndexCountsRemover( storeView, ruleId ),
                logProvider
        );
        proxy = new ContractCheckingIndexProxy( proxy, true );
        return proxy;
    }

    private String indexUserDescription( final IndexDescriptor descriptor,
                                         final SchemaIndexProvider.Descriptor providerDescriptor )
    {
        return format( "%s [provider: %s]",
                descriptor.schema().userDescription( tokenNameLookup ), providerDescriptor.toString() );
    }

    private IndexPopulator populatorFromProvider( SchemaIndexProvider.Descriptor providerDescriptor, long ruleId,
                                                  IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        SchemaIndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getPopulator( ruleId, descriptor, samplingConfig );
    }

    private IndexAccessor onlineAccessorFromProvider( SchemaIndexProvider.Descriptor providerDescriptor,
                                                      long ruleId, IndexDescriptor descriptor,
                                                      IndexSamplingConfig samplingConfig ) throws IOException
    {
        SchemaIndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getOnlineAccessor( ruleId, descriptor, samplingConfig );
    }
}
