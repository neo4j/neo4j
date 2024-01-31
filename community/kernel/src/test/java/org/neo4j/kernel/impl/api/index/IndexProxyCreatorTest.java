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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.api.exceptions.index.FlipFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.InMemoryTokens;

class IndexProxyCreatorTest
{
    private final int LABEL_ID = 7;
    private final int PROPERTY_KEY_ID = 15;

    private final IndexPrototype UNIQUE_INDEX = uniqueForSchema( forLabel( LABEL_ID, PROPERTY_KEY_ID ) )
            .withIndexProvider( PROVIDER_DESCRIPTOR )
            .withName( "constraint" );

    @Test
    void onlineUniquenessIndexProxyWithOwningConstraintShouldBeOnline()
    {
        final var provider = newProvider();
        final var creator = newCreator( provider );

        // when creating an index proxy for an online uniqueness index with an owning constraint
        final var proxy = (ContractCheckingIndexProxy)
                creator.createOnlineIndexProxy( UNIQUE_INDEX.materialise( 10 ).withOwningConstraintId( 4 ) );

        // then we should get an OnlineIndexProxy
        assertThat( proxy.getDelegate() ).isExactlyInstanceOf( OnlineIndexProxy.class );
    }

    @Test
    void onlineUniquenessIndexProxyWithoutOwningConstraintShouldBeTentative()
            throws IndexActivationFailedKernelException
    {
        final var provider = newProvider();
        final var creator = newCreator( provider );

        // when creating an index proxy for an online uniqueness index without an owning constraint
        final var proxy = (ContractCheckingIndexProxy) creator.createOnlineIndexProxy( UNIQUE_INDEX.materialise( 10 ) );
        final var flipper = (FlippableIndexProxy) proxy.getDelegate();

        // then we should get a TentativeConstraintIndexProxy
        assertThat( flipper.getDelegate() ).isExactlyInstanceOf( TentativeConstraintIndexProxy.class );

        // and once activated it should become an OnlineIndexProxy
        proxy.activate();
        assertThat( flipper.getDelegate() ).isExactlyInstanceOf( OnlineIndexProxy.class );
    }

    @Test
    void populatingUniquenessIndexProxyWithOwningConstraintShouldFlipToOnline() throws FlipFailedKernelException
    {
        // when a uniqueness constraint index with an owning constraint completes population
        final FlippableIndexProxy proxy =
                proxyAfterCompletedPopulation( UNIQUE_INDEX.materialise( 10 ).withOwningConstraintId( 4 ) );

        // then it should become an OnlineIndexProxy
        assertThat( proxy.getDelegate() ).isExactlyInstanceOf( OnlineIndexProxy.class );
    }

    @Test
    void populatingUniquenessIndexProxyWithoutOwningConstraintShouldFlipToTentative()
            throws FlipFailedKernelException, IndexActivationFailedKernelException
    {
        // when a uniqueness constraint index without an owning constraint completes population
        final FlippableIndexProxy proxy = proxyAfterCompletedPopulation( UNIQUE_INDEX.materialise( 10 ) );

        // then it should become a TentativeConstraintIndexProxy
        assertThat( proxy.getDelegate() ).isExactlyInstanceOf( TentativeConstraintIndexProxy.class );

        // and once activated it should become an OnlineIndexProxy
        proxy.activate();
        assertThat( proxy.getDelegate() ).isExactlyInstanceOf( OnlineIndexProxy.class );
    }

    private FlippableIndexProxy proxyAfterCompletedPopulation( IndexDescriptor descriptor )
            throws FlipFailedKernelException
    {
        final var provider = newProvider();
        final var creator = newCreator( provider );
        final var job = newJob();

        final var proxy = (ContractCheckingIndexProxy)
                creator.createPopulatingIndexProxy( descriptor, IndexMonitor.NO_MONITOR, job );
        final var flipper = (FlippableIndexProxy) proxy.getDelegate();

        // when population job completes the index is flipped
        flipper.flip( () -> true, mock( FailedIndexProxyFactory.class ) );

        return flipper;
    }

    private IndexProvider newProvider()
    {
        final var provider = mock( IndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        try
        {
            when( provider.getOnlineAccessor( any(), any(), any() ) ).thenReturn( mock( IndexAccessor.class ) );
        }
        catch ( IOException ignored )
        {
        }
        return provider;
    }

    private IndexProxyCreator newCreator( IndexProvider provider )
    {
        return new IndexProxyCreator(
                mock( IndexSamplingConfig.class ),
                mock( IndexStatisticsStore.class ),
                new MockIndexProviderMap( provider ),
                new InMemoryTokens(),
                mock( LogProvider.class ) );
    }

    private IndexPopulationJob newJob()
    {
        return mock( IndexPopulationJob.class );
    }
}
