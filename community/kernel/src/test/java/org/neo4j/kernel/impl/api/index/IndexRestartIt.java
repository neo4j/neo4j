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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;
import static org.neo4j.test.DoubleLatch.awaitLatch;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

public class IndexRestartIt
{
    /* This is somewhat difficult to test since dropping an index while it's populating forces it to be cancelled
     * first (and also awaiting cancellation to complete). So this is a best-effort to have the timing as close
     * as possible. If this proves to be flaky, remove it right away.
     */
    @Test
    public void shouldBeAbleToDropIndexWhileItIsPopulating() throws Exception
    {
        // GIVEN
        startDb();
        DoubleLatch populationCompletionLatch = provider.installPopulationJobCompletionLatch();
        IndexDefinition index = createIndex();
        populationCompletionLatch.awaitStart(); // await population job to start

        // WHEN
        dropIndex( index, populationCompletionLatch );

        // THEN
        assertEquals( asSet(), asSet( db.schema().getIndexes( myLabel ) ) );
        try
        {
            db.schema().getIndexState( index );
            fail( "This index should have been deleted" );
        }
        catch ( NotFoundException e )
        {
            assertThat( e.getMessage(), CoreMatchers.containsString( myLabel.name() ) );
        }
    }
    
    @Test
    public void shouldHandleRestartOfOnlineIndex() throws Exception
    {
        // Given
        startDb();
        createIndex();
        provider.awaitFullyPopulated();

        // And Given
        stopDb();
        provider.setInitialIndexState( ONLINE );

        // When
        startDb();

        // Then
        IndexDefinition index = getSingleIndex();
        assertThat( db.schema().getIndexState( index), equalTo( Schema.IndexState.ONLINE ) );
        assertEquals( 1, provider.populatorCallCount.get() );
        assertEquals( 2, provider.writerCallCount.get() );
    }

    @Test
    public void shouldHandleRestartIndexThatHasNotComeOnlineYet() throws Exception
    {
        // Given
        startDb();
        createIndex();

        // And Given
        stopDb();
        provider.setInitialIndexState( POPULATING );

        // When
        startDb();

        IndexDefinition index = getSingleIndex();
        assertThat( db.schema().getIndexState( index), not( equalTo( Schema.IndexState.FAILED ) ) );
        assertEquals( 2, provider.populatorCallCount.get() );
    }

    private GraphDatabaseAPI db;
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private TestGraphDatabaseFactory factory;
    private final ControlledSchemaIndexProvider provider = new ControlledSchemaIndexProvider();
    private final Label myLabel = label( "MyLabel" );
    
    private void startDb()
    {
        if ( db != null )
            db.shutdown();

        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
    }

    private void stopDb()
    {
        if(db != null)
            db.shutdown();
    }

    @Before
    public void before() throws Exception
    {
        factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        factory.setKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList(
                singleInstanceSchemaIndexProviderFactory( "test", provider ) ) );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private IndexDefinition createIndex()
    {
        Transaction tx = db.beginTx();
        IndexDefinition index = db.schema().indexCreator( myLabel ).on( "number_of_bananas_owned" ).create();
        tx.success();
        tx.finish();
        return index;
    }

    private IndexDefinition getSingleIndex()
    {
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );
        assertThat( indexes.size(), equalTo(1));
        return single( indexes );
    }
    
    private void dropIndex( IndexDefinition index, DoubleLatch populationCompletionLatch )
    {
        Transaction tx = db.beginTx();
        try
        {
            index.drop();
            populationCompletionLatch.finish();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public static final SchemaIndexProvider.Descriptor CONTROLLED_PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor( "controlled", "1.0" );

    private class ControlledSchemaIndexProvider extends SchemaIndexProvider
    {

        private IndexPopulator mockedPopulator = new IndexPopulator.Adapter();
        private final IndexAccessor mockedWriter = mock( IndexAccessor.class );
        private final CountDownLatch writerLatch = new CountDownLatch( 1 );
        private InternalIndexState initialIndexState = POPULATING;
        private final AtomicInteger populatorCallCount = new AtomicInteger();
        private final AtomicInteger writerCallCount = new AtomicInteger();
        
        public ControlledSchemaIndexProvider()
        {
            super( CONTROLLED_PROVIDER_DESCRIPTOR, 10 );
            setInitialIndexState( initialIndexState );
        }
        
        DoubleLatch installPopulationJobCompletionLatch()
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

        void setInitialIndexState( InternalIndexState initialIndexState )
        {
            this.initialIndexState = initialIndexState;
        }

        @Override
        public IndexPopulator getPopulator( long indexId )
        {
            populatorCallCount.incrementAndGet();
            return mockedPopulator;
        }

        @Override
        public IndexAccessor getOnlineAccessor( long indexId )
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
    }
}
