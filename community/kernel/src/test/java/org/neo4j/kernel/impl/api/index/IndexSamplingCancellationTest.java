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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.DelegatingIndexReader;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.index_background_sampling_enabled;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRIGGER_REBUILD_ALL;

public class IndexSamplingCancellationTest
{
    private final Barrier.Control samplingStarted = new Barrier.Control(), samplingDone = new Barrier.Control();
    private volatile Throwable samplingException;
    private final InMemoryIndexProvider index = new TestInMemoryIndexProvider();
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseFactory factory )
        {
            ((TestGraphDatabaseFactory) factory).addKernelExtension( new InMemoryIndexProviderFactory( index ) );
        }

        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( index_background_sampling_enabled, FALSE );
        }
    };

    @Test
    public void shouldStopSamplingWhenIndexIsDropped() throws Exception
    {
        // given
        IndexDefinition index = awaitOnline( indexOn( label( "Foo" ), "bar" ) );

        // when
        db.resolveDependency( IndexingService.class ).triggerIndexSampling( TRIGGER_REBUILD_ALL );
        samplingStarted.await();
        drop( index );
        samplingDone.release();
        samplingStarted.release();
        samplingDone.await();

        // then
        Throwable exception = samplingException;
        assertThat( exception, instanceOf( IndexNotFoundKernelException.class ) );
        assertEquals( "Index dropped while sampling.", exception.getMessage() );
    }

    private void drop( IndexDefinition index )
    {
        try ( Transaction tx = db.beginTx() )
        {
            index.drop();
            tx.success();
        }
    }

    private IndexDefinition indexOn( Label label, String propertyKey )
    {
        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.schema().indexFor( label ).on( propertyKey ).create();
            tx.success();
        }
        return index;
    }

    private IndexDefinition awaitOnline( IndexDefinition index )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 3, SECONDS );

            tx.success();
        }
        return index;
    }

    private class TestInMemoryIndexProvider extends InMemoryIndexProvider
    {
        TestInMemoryIndexProvider()
        {
            super( 100 );
        }

        @Override
        public IndexAccessor getOnlineAccessor( long indexId, IndexDescriptor descriptor,
                IndexSamplingConfig samplingConfig )
        {
            return new DelegatingIndexAccessor( super.getOnlineAccessor( indexId, descriptor, samplingConfig ) );
        }
    }

    private class DelegatingIndexAccessor extends IndexAccessor.Delegator
    {
        DelegatingIndexAccessor( IndexAccessor delegate )
        {
            super( delegate );
        }

        @Override
        public IndexReader newReader()
        {
            return new TestIndexReader( super.newReader() );
        }
    }

    private class TestIndexReader extends DelegatingIndexReader
    {
        TestIndexReader( IndexReader delegate )
        {
            super( delegate );
        }

        @Override
        public IndexSampler createSampler()
        {
            samplingStarted.reached();
            IndexSampler sampler = super.createSampler();
            return new DelegatingIndexSampler( sampler );
        }
    }

    private class DelegatingIndexSampler implements IndexSampler
    {
        final IndexSampler delegate;

        DelegatingIndexSampler( IndexSampler delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public IndexSample sampleIndex() throws IndexNotFoundKernelException
        {
            try
            {
                return delegate.sampleIndex();
            }
            catch ( Throwable e )
            {
                samplingException = e;
                throw e;
            }
            finally
            {
                samplingDone.reached();
            }
        }
    }
}
