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
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.test.DoubleLatch.awaitLatch;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

public class IndexRestartIt
{
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
        if(db != null)
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
        factory.setSchemaIndexProviders( Arrays.<SchemaIndexProvider>asList( provider ) );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private void createIndex()
    {
        Transaction tx = db.beginTx();
        db.schema().indexCreator( myLabel ).on( "number_of_bananas_owned" ).create();
        tx.success();
        tx.finish();
    }

    private IndexDefinition getSingleIndex()
    {
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );
        assertThat( indexes.size(), equalTo(1));
        return single( indexes );
    }
    
    private class ControlledSchemaIndexProvider extends SchemaIndexProvider
    {
        private final IndexPopulator mockedPopulator = mock( IndexPopulator.class );
        private final IndexAccessor mockedWriter = mock( IndexAccessor.class );
        private final CountDownLatch writerLatch = new CountDownLatch( 1 );
        private InternalIndexState initialIndexState = POPULATING;
        private final AtomicInteger populatorCallCount = new AtomicInteger();
        private final AtomicInteger writerCallCount = new AtomicInteger();
        
        public ControlledSchemaIndexProvider()
        {
            super( "test", 10 );
            setInitialIndexState( initialIndexState );
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
        public IndexPopulator getPopulator( long indexId, Dependencies dependencies )
        {
            populatorCallCount.incrementAndGet();
            return mockedPopulator;
        }

        @Override
        public IndexAccessor getOnlineAccessor( long indexId, Dependencies dependencies )
        {
            writerCallCount.incrementAndGet();
            writerLatch.countDown();
            return mockedWriter;
        }

        @Override
        public InternalIndexState getInitialState( long indexId, Dependencies dependencies )
        {
            return initialIndexState;
        }
    }
}
