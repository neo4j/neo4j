/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.sampling;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;

public class OnlineIndexSamplingJobTest
{
    @Test
    public void shouldSampleTheIndexAndStoreTheValueWhenTheIndexIsOnline()
    {
        // given
        OnlineIndexSamplingJob job = new OnlineIndexSamplingJob( indexProxy, indexStoreView, "Foo", logProvider );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        job.run();

        // then
        verify( indexStoreView ).replaceIndexCounts( indexDescriptor, indexUniqueValues, indexSize, indexSize );
        verifyNoMoreInteractions( indexStoreView );
    }

    @Test
    public void shouldSampleTheIndexButDoNotStoreTheValuesIfTheIndexIsNotOnline()
    {
        // given
        OnlineIndexSamplingJob job = new OnlineIndexSamplingJob( indexProxy, indexStoreView, "Foo", logProvider );
        when( indexProxy.getState() ).thenReturn( FAILED );

        // when
        job.run();

        // then
        verifyNoMoreInteractions( indexStoreView );
    }

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final IndexProxy indexProxy = mock( IndexProxy.class );
    private final IndexStoreView indexStoreView = mock( IndexStoreView.class );
    private final IndexDescriptor indexDescriptor = new IndexDescriptor( 1, 2 );
    private final IndexReader indexReader = mock( IndexReader.class );
    private final IndexSampler indexSampler = mock( IndexSampler.class );

    private final long indexUniqueValues = 21L;
    private final long indexSize = 23L;

    @Before
    public void setup() throws IndexNotFoundKernelException
    {
        when( indexProxy.getDescriptor() ).thenReturn( indexDescriptor );
        when( indexProxy.config() ).thenReturn( IndexConfiguration.NON_UNIQUE );
        when( indexProxy.newReader() ).thenReturn( indexReader );
        when( indexReader.createSampler() ).thenReturn( indexSampler );
        when( indexSampler.sampleIndex() ).thenReturn( new IndexSample( indexSize, indexUniqueValues, indexSize ) );
    }
}
