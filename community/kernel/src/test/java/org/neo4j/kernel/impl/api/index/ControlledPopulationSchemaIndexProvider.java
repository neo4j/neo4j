/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.test.DoubleLatch;

import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.test.DoubleLatch.awaitLatch;

public class ControlledPopulationSchemaIndexProvider extends SchemaIndexProvider
{
    private IndexPopulator mockedPopulator = new IndexPopulator.Adapter();
    private final IndexAccessor mockedWriter = mock( IndexAccessor.class );
    private final CountDownLatch writerLatch = new CountDownLatch( 1 );
    private InternalIndexState initialIndexState = POPULATING;
    public final AtomicInteger populatorCallCount = new AtomicInteger();
    public final AtomicInteger writerCallCount = new AtomicInteger();
    
    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR = new SchemaIndexProvider.Descriptor(
            "controlled-population", "1.0" );
    
    public ControlledPopulationSchemaIndexProvider()
    {
        super( PROVIDER_DESCRIPTOR, 10 );
        setInitialIndexState( initialIndexState );
    }
    
    public DoubleLatch installPopulationJobCompletionLatch()
    {
        final DoubleLatch populationCompletionLatch = new DoubleLatch();
        mockedPopulator = new IndexPopulator.Adapter()
        {
            @Override
            public void create() throws IOException
            {
                populationCompletionLatch.startAndAwaitFinish();
                super.create();
            }
        };
        return populationCompletionLatch;
    }
    
    public void awaitFullyPopulated()
    {
        awaitLatch( writerLatch );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexConfiguration config )
    {
        populatorCallCount.incrementAndGet();
        return mockedPopulator;
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config )
    {
        writerCallCount.incrementAndGet();
        writerLatch.countDown();
        return mockedWriter;
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        return initialIndexState;
    }

    public void setInitialIndexState( InternalIndexState initialIndexState )
    {
        this.initialIndexState = initialIndexState;
    }
}