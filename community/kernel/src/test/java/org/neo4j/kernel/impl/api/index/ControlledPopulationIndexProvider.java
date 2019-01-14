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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.test.DoubleLatch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.test.DoubleLatch.awaitLatch;

public class ControlledPopulationIndexProvider extends IndexProvider
{
    private IndexPopulator mockedPopulator = new IndexPopulator.Adapter();
    private final IndexAccessor mockedWriter = mock( IndexAccessor.class );
    private final CountDownLatch writerLatch = new CountDownLatch( 1 );
    private InternalIndexState initialIndexState = POPULATING;
    final AtomicInteger populatorCallCount = new AtomicInteger();
    final AtomicInteger writerCallCount = new AtomicInteger();

    public static final IndexProvider.Descriptor PROVIDER_DESCRIPTOR = new IndexProvider.Descriptor(
            "controlled-population", "1.0" );

    public ControlledPopulationIndexProvider()
    {
        super( PROVIDER_DESCRIPTOR, 10, IndexDirectoryStructure.NONE );
        setInitialIndexState( initialIndexState );
        when( mockedWriter.newReader() ).thenReturn( IndexReader.EMPTY );
    }

    public DoubleLatch installPopulationJobCompletionLatch()
    {
        final DoubleLatch populationCompletionLatch = new DoubleLatch();
        mockedPopulator = new IndexPopulator.Adapter()
        {
            @Override
            public void create() throws IOException
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
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        populatorCallCount.incrementAndGet();
        return mockedPopulator;
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor indexConfig,
                                            IndexSamplingConfig samplingConfig )
    {
        writerCallCount.incrementAndGet();
        writerLatch.countDown();
        return mockedWriter;
    }

    @Override
    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
    {
        return initialIndexState;
    }

    @Override
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        return IndexCapability.NO_CAPABILITY;
    }

    public void setInitialIndexState( InternalIndexState initialIndexState )
    {
        this.initialIndexState = initialIndexState;
    }

    @Override
    public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
    {
        throw new IllegalStateException();
    }

    @Override
    public int compareTo( IndexProvider o )
    {
        return 1;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }
}
