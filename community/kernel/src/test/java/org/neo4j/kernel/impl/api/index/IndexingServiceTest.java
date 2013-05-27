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
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.xa.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.logging.Logging;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;
import static org.neo4j.test.AwaitAnswer.afterAwaiting;

public class IndexingServiceTest
{
    @Rule
    public final LifeRule life = new LifeRule();
    private long labelId;
    private long propertyKeyId;

    @Test
    public void shouldBringIndexOnlineAndFlipOverToIndexAccessor() throws Exception
    {
        // given
        labelId = 7;
        propertyKeyId = 15;
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexAccessor accessor = mock( IndexAccessor.class );

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndex( IndexRule.indexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        IndexProxy proxy = indexingService.getProxyForRule( 0 );

        verify( populator, timeout( 1000 ) ).close( true );
        proxy.update( withData(
                add( 10, "foo" )
        ) );

        // then
        assertEquals( InternalIndexState.ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor );
        order.verify( populator ).create();
        order.verify( populator ).close( true );
        order.verify( accessor ).updateAndCommit( argThat( containsAll( withData(
                add( 10, "foo" )
        ) ) ) );
    }

    @Test
    public void shouldDeliverUpdatesThatOccurDuringPopulationToPopulator() throws Exception
    {
        // given
        labelId = 7;
        propertyKeyId = 15;
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexAccessor accessor = mock( IndexAccessor.class );

        CountDownLatch latch = new CountDownLatch( 1 );
        doAnswer( afterAwaiting( latch ) ).when( populator ).add( anyLong(), any() );

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData(
                add( 1, "value1" )
        ) );

        life.start();

        // when
        indexingService.createIndex( IndexRule.indexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        IndexProxy proxy = indexingService.getProxyForRule( 0 );
        assertEquals( InternalIndexState.POPULATING, proxy.getState() );
        proxy.update( withData(
                add( 2, "value2" )
        ) );
        latch.countDown();

        verify( populator, timeout( 1000 ) ).close( true );

        // then
        assertEquals( InternalIndexState.ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor );
        order.verify( populator ).create();
        order.verify( populator ).add( 1, "value1" );
        order.verify( populator ).update( argThat( containsAll( withData(
                // this is invoked from indexAllNodes(),
                // empty because the id we added (2) is bigger than the one we indexed (1)
        ) ) ) );
        order.verify( populator ).update( argThat( containsAll( withData(
                add( 2, "value2" )
        ) ) ) );
        order.verify( populator ).close( true );
        verifyNoMoreInteractions( populator );
        verifyZeroInteractions( accessor );
    }

    @Test
    public void shouldStillReportInternalIndexStateAsPopulatingWhenConstraintIndexIsDonePopulating() throws Exception
    {
        // given
        labelId = 7;
        propertyKeyId = 15;
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexAccessor accessor = mock( IndexAccessor.class );

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndex( IndexRule.constraintIndexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR,
                                                                    null ) );
        IndexProxy proxy = indexingService.getProxyForRule( 0 );

        verify( populator, timeout( 1000 ) ).close( true );
        proxy.update( withData(
                add( 10, "foo" )
        ) );

        // then
        assertEquals( InternalIndexState.POPULATING, proxy.getState() );
        InOrder order = inOrder( populator, accessor );
        order.verify( populator ).create();
        order.verify( populator ).close( true );
        order.verify( accessor ).updateAndCommit( argThat( containsAll( withData(
                add( 10, "foo" )
        ) ) ) );
    }

    @Test
    public void shouldBringConstraintIndexOnlineWhenExplicitlyToldTo() throws Exception
    {
        // given
        labelId = 7;
        propertyKeyId = 15;
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexAccessor accessor = mock( IndexAccessor.class );

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndex( IndexRule.constraintIndexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR,
                                                                    null ) );
        IndexProxy proxy = indexingService.getProxyForRule( 0 );

        verify( populator, timeout( 1000 ) ).close( true );

        indexingService.activateIndex( 0 );

        // then
        assertEquals( InternalIndexState.ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor );
        order.verify( populator ).create();
        order.verify( populator ).close( true );
    }

    @Test
    public void shouldLogIndexStateOnInit() throws Exception
    {
        // given
        TestLogger logger = new TestLogger();
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        IndexingService indexingService = new IndexingService(
                mock( JobScheduler.class ),
                providerMap,
                mock( IndexStoreView.class ),
                mock( UpdateableSchemaState.class ),
                mockLogging( logger ) );

        IndexRule onlineIndex = IndexRule.indexRule( 1, 1, 1, PROVIDER_DESCRIPTOR );
        IndexRule populatingIndex = IndexRule.indexRule( 2, 2, 2, PROVIDER_DESCRIPTOR );
        IndexRule failedIndex = IndexRule.indexRule( 3, 3, 3, PROVIDER_DESCRIPTOR );

        when( provider.getInitialState( onlineIndex.getId() ) ).thenReturn( InternalIndexState.ONLINE );
        when( provider.getInitialState( populatingIndex.getId() ) ).thenReturn( InternalIndexState.POPULATING );
        when( provider.getInitialState( failedIndex.getId() ) ).thenReturn( InternalIndexState.FAILED );

        // when
        indexingService.initIndexes( asList( onlineIndex, populatingIndex, failedIndex ).iterator() );

        // then
        logger.assertExactly(
                info( "IndexingService.initIndexes: index on :label[1](property[1]) is ONLINE" ),
                info( "IndexingService.initIndexes: index on :label[2](property[2]) is POPULATING" ),
                info( "IndexingService.initIndexes: index on :label[3](property[3]) is FAILED" )
        );
    }

    @Test
    public void shouldLogIndexStateOnStart() throws Exception
    {
        // given
        TestLogger logger = new TestLogger();
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        IndexingService indexingService = new IndexingService(
                mock( JobScheduler.class ),
                providerMap,
                mock( IndexStoreView.class ),
                mock( UpdateableSchemaState.class ),
                mockLogging( logger ) );

        IndexRule onlineIndex = IndexRule.indexRule( 1, 1, 1, PROVIDER_DESCRIPTOR );
        IndexRule populatingIndex = IndexRule.indexRule( 2, 2, 2, PROVIDER_DESCRIPTOR );
        IndexRule failedIndex = IndexRule.indexRule( 3, 3, 3, PROVIDER_DESCRIPTOR );

        when( provider.getInitialState( onlineIndex.getId() ) ).thenReturn( InternalIndexState.ONLINE );
        when( provider.getInitialState( populatingIndex.getId() ) ).thenReturn( InternalIndexState.POPULATING );
        when( provider.getInitialState( failedIndex.getId() ) ).thenReturn( InternalIndexState.FAILED );
        indexingService.initIndexes( asList( onlineIndex, populatingIndex, failedIndex ).iterator() );

        logger.clear();

        // when
        indexingService.start();

        // then
        logger.assertAtLeastOnce(
                info( "IndexingService.start: index on :label[1](property[1]) is ONLINE" ) );
        logger.assertAtLeastOnce(
                info( "IndexingService.start: index on :label[2](property[2]) is POPULATING" ) );
        logger.assertAtLeastOnce(
                info( "IndexingService.start: index on :label[3](property[3]) is FAILED" ) );
    }

    private static Logging mockLogging( StringLogger logger )
    {
        Logging logging = mock( Logging.class );
        when( logging.getMessagesLog( any( Class.class ) ) ).thenReturn( logger );
        return logging;
    }

    private NodePropertyUpdate add( long nodeId, Object propertyValue )
    {
        return NodePropertyUpdate.add( nodeId, propertyKeyId, propertyValue, new long[]{labelId} );
    }

    private IndexingService newIndexingServiceWithMockedDependencies( IndexPopulator populator,
                                                                      IndexAccessor accessor,
                                                                      DataUpdates data ) throws IOException
    {
        StringLogger logger = mock( StringLogger.class );
        SchemaIndexProvider indexProvider = mock( SchemaIndexProvider.class );
        IndexStoreView storeView = mock( IndexStoreView.class );
        UpdateableSchemaState schemaState = mock( UpdateableSchemaState.class );

        when( indexProvider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( indexProvider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( populator );
        data.getsProcessedByStoreScanFrom( storeView );
        when( indexProvider.getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( accessor );

        return life.add( new IndexingService(
                life.add( new Neo4jJobScheduler( logger ) ), new DefaultSchemaIndexProviderMap( indexProvider ),
                storeView, schemaState, mockLogging( logger ) ) );
    }

    private DataUpdates withData( NodePropertyUpdate... updates )
    {
        return new DataUpdates( updates );
    }

    private static class DataUpdates implements Answer<StoreScan>, Iterable<NodePropertyUpdate>
    {
        private final NodePropertyUpdate[] updates;

        DataUpdates( NodePropertyUpdate[] updates )
        {
            this.updates = updates;
        }

        void getsProcessedByStoreScanFrom( IndexStoreView mock )
        {
            when( mock.visitNodesWithPropertyAndLabel( any( IndexDescriptor.class ), visitor( any( Visitor.class ) ) ) )
                    .thenAnswer( this );
        }

        @Override
        public StoreScan answer( InvocationOnMock invocation ) throws Throwable
        {
            final Visitor<NodePropertyUpdate, RuntimeException> visitor = visitor( invocation.getArguments()[1] );
            return new StoreScan()
            {
                @Override
                public void run()
                {
                    for ( NodePropertyUpdate update : updates )
                    {
                        visitor.visit( update );
                    }
                }

                @Override
                public void stop()
                {
                    throw new UnsupportedOperationException( "not implemented" );
                }
            };
        }

        @SuppressWarnings("unchecked")
        private static Visitor<NodePropertyUpdate, RuntimeException> visitor( Object v )
        {
            return (Visitor) v;
        }

        @Override
        public Iterator<NodePropertyUpdate> iterator()
        {
            return new ArrayIterator<NodePropertyUpdate>( updates );
        }

        @Override
        public String toString()
        {
            return Arrays.toString( updates );
        }
    }

    private static Matcher<Iterable<NodePropertyUpdate>> containsAll( final DataUpdates updates )
    {
        return new TypeSafeMatcher<Iterable<NodePropertyUpdate>>()
        {
            @Override
            protected boolean matchesSafely( Iterable<NodePropertyUpdate> item )
            {
                return asSet( updates ).equals( asSet( item ) );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( updates );
            }
        };
    }
}
