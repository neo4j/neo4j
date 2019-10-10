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
package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.test.DoubleLatch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.test.DoubleLatch.awaitLatch;

public class ControlledPopulationIndexProvider extends IndexProvider.Adaptor
{
    private IndexPopulator mockedPopulator = new IndexPopulator.Adapter();
    private final IndexAccessor mockedWriter = mock( IndexAccessor.class );
    private final CountDownLatch writerLatch = new CountDownLatch( 1 );
    private InternalIndexState initialIndexState = POPULATING;
    final AtomicInteger populatorCallCount = new AtomicInteger();
    final AtomicInteger writerCallCount = new AtomicInteger();

    public static final IndexProviderDescriptor PROVIDER_DESCRIPTOR = new IndexProviderDescriptor(
            "controlled-population", "1.0" );

    public ControlledPopulationIndexProvider()
    {
        super( PROVIDER_DESCRIPTOR, IndexDirectoryStructure.NONE );
        setInitialIndexState( initialIndexState );
        when( mockedWriter.newReader() ).thenReturn( IndexReader.EMPTY );
    }

    public DoubleLatch installPopulationJobCompletionLatch()
    {
        final DoubleLatch populationCompletionLatch = new DoubleLatch();
        mockedPopulator = new IndexPopulator.Adapter()
        {
            @Override
            public void create()
            {
                populationCompletionLatch.startAndWaitForAllToStartAndFinish();
                super.create();
            }

            @Override
            public IndexSample sampleResult()
            {
                return new IndexSample();
            }
        };
        return populationCompletionLatch;
    }

    public void awaitFullyPopulated()
    {
        awaitLatch( writerLatch );
    }

    @Override
    public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory )
    {
        populatorCallCount.incrementAndGet();
        return mockedPopulator;
    }

    @Override
    public IndexAccessor getOnlineAccessor( IndexDescriptor indexConfig, IndexSamplingConfig samplingConfig )
    {
        writerCallCount.incrementAndGet();
        writerLatch.countDown();
        return mockedWriter;
    }

    @Override
    public InternalIndexState getInitialState( IndexDescriptor descriptor )
    {
        return initialIndexState;
    }

    public void setInitialIndexState( InternalIndexState initialIndexState )
    {
        this.initialIndexState = initialIndexState;
    }
}
