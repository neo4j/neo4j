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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.test.DoubleLatch.awaitLatch;

public class ControlledPopulationSchemaIndexProvider extends SchemaIndexProvider
{
    private final SchemaIndexProvider delegate;

    private IndexPopulator mockedPopulator = null;
    private IndexAccessor mockedWriter = null;

    private DoubleLatch populationCompletionLatch = null;

    private final CountDownLatch writerLatch = new CountDownLatch( 1 );

    private InternalIndexState initialIndexState = POPULATING;
    public final AtomicInteger populatorCallCount = new AtomicInteger();
    public final AtomicInteger writerCallCount = new AtomicInteger();

    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR = new SchemaIndexProvider.Descriptor(
            "controlled-population", "1.0" );
    

    public ControlledPopulationSchemaIndexProvider( SchemaIndexProvider delegate )
    {
        super( PROVIDER_DESCRIPTOR, 10 );
        setInitialIndexState( initialIndexState );
        this.delegate = delegate;
    }

    public ControlledPopulationSchemaIndexProvider()
    {
        this( mockIndexProvider() );
    }

    private static SchemaIndexProvider mockIndexProvider()
    {
        SchemaIndexProvider result = mock(SchemaIndexProvider.class);
        when( result.getPopulator( anyLong(), any( IndexConfiguration.class ) ) )
            .thenReturn( new IndexPopulator.Adapter() );
        when( result.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );

        try
        {
            when( result.getOnlineAccessor( anyLong(), any( IndexConfiguration.class) ) )
                .thenReturn( mock( IndexAccessor.class ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return result;
    }

    public DoubleLatch installPopulationJobCompletionLatch( )
    {
        if ( null == populationCompletionLatch )
        {
            populationCompletionLatch = new DoubleLatch();
        }
        else
        {
            throw new IllegalStateException( "Invalid attempt to install completion latch twice" );
        }
        return populationCompletionLatch;
    }

    public void awaitWriterStarted()
    {
        awaitLatch( writerLatch );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexConfiguration config )
    {
        if ( null == mockedPopulator )
        {
            mockedPopulator = delegate.getPopulator( indexId, config );

            if ( null != populationCompletionLatch )
            {
                final IndexPopulator oldPopulator = mockedPopulator;
                mockedPopulator = new IndexPopulator.Adapter()
                {
                    @Override
                    public void create() throws IOException
                    {
                        populationCompletionLatch.startAndAwaitFinish();
                        oldPopulator.create();
                    }

                    @Override
                    public void close( boolean populationCompletedSuccessfully ) throws IOException
                    {
                        oldPopulator.close( populationCompletedSuccessfully );

                        if ( null != populationCompletionLatch )
                        {
                            populationCompletionLatch.finish();
                        }
                    }
                };
            }
        }
        populatorCallCount.incrementAndGet();
        return mockedPopulator;
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config ) throws IOException
    {
        if ( null == mockedWriter )
        {
            mockedWriter = delegate.getOnlineAccessor( indexId, config );
        }
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

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        throw new IllegalStateException();
    }
}