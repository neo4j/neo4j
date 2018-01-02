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

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

public class IndexProxySetup
{
    private final IndexSamplingConfig samplingConfig;
    private final IndexStoreView storeView;
    private final SchemaIndexProviderMap providerMap;
    private final UpdateableSchemaState updateableSchemaState;
    private final TokenNameLookup tokenNameLookup;
    private final JobScheduler scheduler;
    private final LogProvider logProvider;

    public IndexProxySetup( IndexSamplingConfig samplingConfig,
                            IndexStoreView storeView,
                            SchemaIndexProviderMap providerMap,
                            UpdateableSchemaState updateableSchemaState,
                            TokenNameLookup tokenNameLookup,
                            JobScheduler scheduler,
                            LogProvider logProvider )
    {
        this.samplingConfig = samplingConfig;
        this.storeView = storeView;
        this.providerMap = providerMap;
        this.updateableSchemaState = updateableSchemaState;
        this.tokenNameLookup = tokenNameLookup;
        this.scheduler = scheduler;
        this.logProvider = logProvider;
    }

    public IndexProxy createPopulatingIndexProxy( final long ruleId,
                                                  final IndexDescriptor descriptor,
                                                  final SchemaIndexProvider.Descriptor providerDescriptor,
                                                  final IndexConfiguration config,
                                                  final boolean flipToTentative,
                                                  final IndexingService.Monitor monitor ) throws IOException
    {
        final FlippableIndexProxy flipper = new FlippableIndexProxy();

        // TODO: This is here because there is a circular dependency from PopulatingIndexProxy to FlippableIndexProxy
        final String indexUserDescription = indexUserDescription( descriptor, providerDescriptor );
        final IndexPopulator populator = populatorFromProvider( providerDescriptor, ruleId, descriptor, config,
                samplingConfig );

        FailedIndexProxyFactory failureDelegateFactory = new FailedPopulatingIndexProxyFactory(
                descriptor,
                config,
                providerDescriptor,
                populator,
                indexUserDescription,
                IndexCountsRemover.Factory.create( storeView, descriptor ),
                logProvider
        );

        PopulatingIndexProxy populatingIndex =
                new PopulatingIndexProxy( scheduler, descriptor, config, failureDelegateFactory, populator, flipper,
                        storeView, updateableSchemaState, logProvider, indexUserDescription, providerDescriptor, monitor );
        flipper.flipTo( populatingIndex );

        // Prepare for flipping to online mode
        flipper.setFlipTarget( new IndexProxyFactory()
        {
            @Override
            public IndexProxy create() throws Exception
            {
                monitor.populationCompleteOn( descriptor );
                OnlineIndexProxy onlineProxy = new OnlineIndexProxy(
                        descriptor, config, onlineAccessorFromProvider( providerDescriptor, ruleId,
                        config, samplingConfig ), storeView, providerDescriptor );
                if ( flipToTentative )
                {
                    return new TentativeConstraintIndexProxy( flipper, onlineProxy );
                }
                return onlineProxy;
            }
        } );

        return new ContractCheckingIndexProxy( flipper, false );
    }

    public IndexProxy createRecoveringIndexProxy( IndexDescriptor descriptor,
                                                  SchemaIndexProvider.Descriptor providerDescriptor,
                                                  boolean constraint )
    {
        IndexConfiguration configuration = new IndexConfiguration( constraint );
        IndexProxy proxy;
        proxy = new RecoveringIndexProxy( descriptor, providerDescriptor, configuration );
        proxy = new ContractCheckingIndexProxy( proxy, true );
        return proxy;
    }

    public IndexProxy createOnlineIndexProxy( long ruleId,
                                              IndexDescriptor descriptor,
                                              SchemaIndexProvider.Descriptor providerDescriptor,
                                              boolean unique )
    {
        // TODO Hook in version verification/migration calls to the SchemaIndexProvider here
        try
        {
            IndexConfiguration config = new IndexConfiguration( unique );
            IndexAccessor onlineAccessor =
                    onlineAccessorFromProvider( providerDescriptor, ruleId, config, samplingConfig );
            IndexProxy proxy;
            proxy = new OnlineIndexProxy( descriptor, config, onlineAccessor, storeView, providerDescriptor );
            proxy = new ContractCheckingIndexProxy( proxy, true );
            return proxy;
        }
        catch ( IOException e )
        {
            logProvider.getLog( getClass() ).error( "Failed to open index: " + ruleId +
                    " (" + descriptor.userDescription( tokenNameLookup ) +
                    "), requesting re-population.", e );
            return createRecoveringIndexProxy( descriptor, providerDescriptor, unique );
        }
    }

    public IndexProxy createFailedIndexProxy( long ruleId,
                                              IndexDescriptor descriptor,
                                              SchemaIndexProvider.Descriptor providerDescriptor,
                                              boolean unique,
                                              IndexPopulationFailure populationFailure )
    {
        IndexConfiguration config = new IndexConfiguration( unique );
        IndexPopulator indexPopulator =
                populatorFromProvider( providerDescriptor, ruleId, descriptor, config, samplingConfig );
        String indexUserDescription = indexUserDescription(descriptor, providerDescriptor);
        IndexProxy proxy;
        proxy = new FailedIndexProxy(
                descriptor,
                config,
                providerDescriptor,
                indexUserDescription,
                indexPopulator,
                populationFailure,
                IndexCountsRemover.Factory.create( storeView, descriptor ),
                logProvider
        );
        proxy = new ContractCheckingIndexProxy( proxy, true );
        return proxy;
    }

    public String indexStateInfo( String tag, Long indexId, InternalIndexState state, IndexDescriptor descriptor )
    {
        return format( "IndexingService.%s: index %d on %s is %s", tag, indexId, indexUserDescription( descriptor ), state.name() );
    }

    public String indexUserDescription( final IndexDescriptor descriptor,
                                         final SchemaIndexProvider.Descriptor providerDescriptor )
    {
        return format( "%s [provider: %s]", indexUserDescription( descriptor ), providerDescriptor.toString() );
    }

    public String indexUserDescription( final IndexDescriptor descriptor )
    {
        return descriptor.userDescription( tokenNameLookup );
    }

    private IndexPopulator populatorFromProvider( SchemaIndexProvider.Descriptor providerDescriptor, long ruleId,
                                                  IndexDescriptor descriptor, IndexConfiguration indexConfig,
                                                  IndexSamplingConfig samplingConfig )
    {
        SchemaIndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getPopulator( ruleId, descriptor, indexConfig, samplingConfig );
    }

    private IndexAccessor onlineAccessorFromProvider( SchemaIndexProvider.Descriptor providerDescriptor,
                                                      long ruleId, IndexConfiguration indexConfig,
                                                      IndexSamplingConfig samplingConfig ) throws IOException
    {
        SchemaIndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getOnlineAccessor( ruleId, indexConfig, samplingConfig );
    }
}
