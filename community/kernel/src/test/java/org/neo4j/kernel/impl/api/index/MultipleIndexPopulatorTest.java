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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator.IndexPopulation;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;

@RunWith( MockitoJUnitRunner.class )
public class MultipleIndexPopulatorTest
{
    @Mock
    private IndexStoreView indexStoreView;
    @Mock
    private StoreScan storeScan;
    @Mock( answer = Answers.RETURNS_MOCKS )
    private LogProvider logProvider;
    @InjectMocks
    private MultipleIndexPopulator multipleIndexPopulator;

    private final LabelSchemaDescriptor index1 = SchemaDescriptorFactory.forLabel( 1, 1 );

    @Before
    public void setUp()
    {
        when( indexStoreView.visitNodes( any( int[].class ), any( IntPredicate.class ), any(Visitor.class),
                any(Visitor.class), anyBoolean() ) ).thenReturn( storeScan );
    }

    @Test
    public void testMultiplePopulatorsCreation() throws Exception
    {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();
        addPopulator( indexPopulator1, 1 );
        addPopulator( indexPopulator2, 2 );

        multipleIndexPopulator.create();

        verify( indexPopulator1 ).create();
        verify( indexPopulator2 ).create();
    }

    @Test
    public void testMultiplePopulatorCreationFailure() throws IOException
    {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();
        IndexPopulator indexPopulator3 = createIndexPopulator();

        doThrow( getPopulatorException() ).when( indexPopulator1 ).create();
        doThrow( getPopulatorException() ).when( indexPopulator3 ).create();

        addPopulator( indexPopulator1, 1 );
        addPopulator( indexPopulator2, 2 );
        addPopulator( indexPopulator3, 3 );

        multipleIndexPopulator.create();

        checkPopulatorFailure( indexPopulator1 );
        checkPopulatorFailure( indexPopulator3 );

        verify( indexPopulator2 ).create();
    }

    @Test
    public void testHasPopulators()
    {
        assertFalse( multipleIndexPopulator.hasPopulators() );

        addPopulator( createIndexPopulator(), 42 );

        assertTrue( multipleIndexPopulator.hasPopulators() );
    }

    @Test
    public void testIndexAllNodes()
    {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator( indexPopulator1, 1 );
        addPopulator( indexPopulator2, 2 );

        multipleIndexPopulator.create();
        multipleIndexPopulator.indexAllNodes();

        verify( indexStoreView ).visitNodes( any(int[].class), any( IntPredicate.class ), any( Visitor.class ),
                any( Visitor.class ), anyBoolean() );
    }

    @Test
    public void testFailPopulator() throws IOException
    {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator( indexPopulator1, 1 );
        addPopulator( indexPopulator2, 2 );

        multipleIndexPopulator.fail( getPopulatorException() );

        checkPopulatorFailure( indexPopulator1 );
        checkPopulatorFailure( indexPopulator2 );
    }

    @Test
    public void testFailByPopulation() throws IOException
    {
        IndexPopulator populator1 = createIndexPopulator();
        IndexPopulator populator2 = createIndexPopulator();

        addPopulator( populator1, 1 );
        IndexPopulation population2 = addPopulator( populator2, 2 );

        multipleIndexPopulator.fail( population2, getPopulatorException() );

        verify( populator1, never() ).markAsFailed( anyString() );
        checkPopulatorFailure( populator2 );
    }

    @Test
    public void testFailByPopulationRemovesPopulator() throws IOException
    {
        IndexPopulator populator1 = createIndexPopulator();
        IndexPopulator populator2 = createIndexPopulator();

        IndexPopulation population1 = addPopulator( populator1, 1 );
        IndexPopulation population2 = addPopulator( populator2, 2 );

        multipleIndexPopulator.fail( population1, getPopulatorException() );
        multipleIndexPopulator.fail( population2, getPopulatorException() );

        checkPopulatorFailure( populator1 );
        checkPopulatorFailure( populator2 );
        assertFalse( multipleIndexPopulator.hasPopulators() );
    }

    @Test
    public void testFailByNonExistingPopulation() throws IOException
    {
        IndexPopulation nonExistingPopulation = mock( IndexPopulation.class );
        when( nonExistingPopulation.schema() ).thenReturn( SchemaDescriptorFactory.forLabel( 1, 1 ) );

        IndexPopulator populator = createIndexPopulator();

        addPopulator( populator, 1 );

        multipleIndexPopulator.fail( nonExistingPopulation, getPopulatorException() );

        verify( populator, never() ).markAsFailed( anyString() );
    }

    @Test
    public void closeMultipleIndexPopulator()
            throws IOException
    {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator( indexPopulator1, 1 );
        addPopulator( indexPopulator2, 2 );

        doThrow( getPopulatorException() ).when( indexPopulator2 ).close( true );

        multipleIndexPopulator.close( true );

        verify( indexPopulator1 ).close( true );
        checkPopulatorFailure( indexPopulator2 );
    }

    @Test
    public void testFlipAfterPopulation() throws Exception
    {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        FlippableIndexProxy flipper1 = addPopulator( indexPopulator1, 1 ).flipper;
        FlippableIndexProxy flipper2 = addPopulator( indexPopulator2, 2 ).flipper;

        multipleIndexPopulator.flipAfterPopulation();

        verify( flipper1 ).flip( any( Callable.class ), any( FailedIndexProxyFactory.class ) );
        verify( flipper2 ).flip( any( Callable.class ), any( FailedIndexProxyFactory.class ) );
    }

    @Test
    public void populationsRemovedDuringFlip() throws Exception
    {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator( indexPopulator1, 1 );
        addPopulator( indexPopulator2, 2 );

        assertTrue( multipleIndexPopulator.hasPopulators() );

        multipleIndexPopulator.flipAfterPopulation();

        assertFalse( multipleIndexPopulator.hasPopulators() );
    }

    @Test
    public void testCancelPopulation() throws IOException
    {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator( indexPopulator1, 1 );
        addPopulator( indexPopulator2, 2 );

        multipleIndexPopulator.cancel();

        verify( indexStoreView, times( 2 ) )
                .replaceIndexCounts( anyLong(), eq( 0L ), eq( 0L ), eq( 0L ) );
        verify( indexPopulator1 ).close( false );
        verify( indexPopulator2 ).close( false );
    }

    @Test
    public void testIndexFlip() throws IOException
    {
        IndexProxyFactory indexProxyFactory = mock( IndexProxyFactory.class );
        FailedIndexProxyFactory failedIndexProxyFactory = mock( FailedIndexProxyFactory.class );
        FlippableIndexProxy flipper = new FlippableIndexProxy();
        flipper.setFlipTarget( indexProxyFactory );

        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();
        addPopulator( indexPopulator1, 1, flipper, failedIndexProxyFactory );
        addPopulator( indexPopulator2, 2, flipper, failedIndexProxyFactory );

        when( indexPopulator1.sampleResult() ).thenThrow( getSampleError() );

        multipleIndexPopulator.indexAllNodes();
        multipleIndexPopulator.flipAfterPopulation();

        verify( indexPopulator1 ).close( false );
        verify( failedIndexProxyFactory, times( 1 ) ).create( any( RuntimeException.class ) );

        verify( indexPopulator2 ).close( true );
        verify( indexPopulator2 ).sampleResult();
        verify( indexStoreView ).replaceIndexCounts( anyLong(), anyLong(), anyLong(), anyLong() );
    }

    @Test
    public void testMultiplePopulatorUpdater()
            throws IOException, IndexEntryConflictException
    {
        IndexUpdater indexUpdater1 = mock( IndexUpdater.class );
        IndexPopulator indexPopulator1 = createIndexPopulator( indexUpdater1 );
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator( indexPopulator1, 1 );
        addPopulator( indexPopulator2, 2 );

        doThrow( getPopulatorException() ).when( indexPopulator2 )
                .newPopulatingUpdater( any( PropertyAccessor.class ) );

        IndexUpdater multipleIndexUpdater =
                multipleIndexPopulator.newPopulatingUpdater( mock( PropertyAccessor.class ) );
        IndexEntryUpdate<?> propertyUpdate = createIndexEntryUpdate( index1 );
        multipleIndexUpdater.process( propertyUpdate );

        checkPopulatorFailure( indexPopulator2 );
        verify( indexUpdater1 ).process( propertyUpdate );
    }

    @Test
    public void testNonApplicableUpdaterDoNotUpdatePopulator()
            throws IOException, IndexEntryConflictException
    {
        IndexUpdater indexUpdater1 = mock( IndexUpdater.class );
        IndexPopulator indexPopulator1 = createIndexPopulator( indexUpdater1 );

        addPopulator( indexPopulator1, 2 );

        IndexUpdater multipleIndexUpdater =
                multipleIndexPopulator.newPopulatingUpdater( mock( PropertyAccessor.class ) );

        IndexEntryUpdate<?> propertyUpdate = createIndexEntryUpdate( index1 );
        multipleIndexUpdater.process( propertyUpdate );

        verifyZeroInteractions( indexUpdater1 );
    }

    @Test
    public void testPropertyUpdateFailure()
            throws IOException, IndexEntryConflictException
    {
        IndexEntryUpdate<?> propertyUpdate = createIndexEntryUpdate( index1 );
        IndexUpdater indexUpdater1 = mock( IndexUpdater.class );
        IndexPopulator indexPopulator1 = createIndexPopulator( indexUpdater1 );

        addPopulator( indexPopulator1, 1 );

        doThrow( getPopulatorException() ).when( indexUpdater1 ).process( propertyUpdate );

        IndexUpdater multipleIndexUpdater =
                multipleIndexPopulator.newPopulatingUpdater( mock( PropertyAccessor.class ) );

        multipleIndexUpdater.process( propertyUpdate );

        verify( indexUpdater1 ).close();
        checkPopulatorFailure( indexPopulator1 );
    }

    @Test
    public void testMultiplePropertyUpdateFailures()
            throws IOException, IndexEntryConflictException
    {
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        IndexEntryUpdate<?> update1 = add( 1, index1, "foo" );
        IndexEntryUpdate<?> update2 = add( 2, index1, "bar" );
        IndexUpdater updater = mock( IndexUpdater.class );
        IndexPopulator populator = createIndexPopulator( updater );

        addPopulator( populator, 1 );

        doThrow( getPopulatorException() ).when( updater ).process( any( IndexEntryUpdate.class ) );

        IndexUpdater multipleIndexUpdater = multipleIndexPopulator.newPopulatingUpdater( propertyAccessor );

        multipleIndexUpdater.process( update1 );
        multipleIndexUpdater.process( update2 );

        verify( updater ).process( update1 );
        verify( updater, never() ).process( update2 );
        verify( updater ).close();
        checkPopulatorFailure( populator );
    }

    private IndexEntryUpdate<?> createIndexEntryUpdate( LabelSchemaDescriptor schemaDescriptor )
    {
        return add( 1, schemaDescriptor, "theValue" );
    }

    private RuntimeException getSampleError()
    {
        return new RuntimeException( "sample error" );
    }

    private IndexPopulator createIndexPopulator( IndexUpdater indexUpdater ) throws IOException
    {
        IndexPopulator indexPopulator = createIndexPopulator();
        when( indexPopulator.newPopulatingUpdater( any( PropertyAccessor.class ) ) ).thenReturn( indexUpdater );
        return indexPopulator;
    }

    private IndexPopulator createIndexPopulator()
    {
        IndexPopulator populator = mock( IndexPopulator.class );
        when( populator.sampleResult() ).thenReturn( new IndexSample() );
        return populator;
    }

    private IOException getPopulatorException()
    {
        return new IOException( "something went wrong" );
    }

    private void checkPopulatorFailure( IndexPopulator populator )
            throws IOException
    {
        verify( populator ).markAsFailed( startsWith( "java.io.IOException: something went wrong" ) );
        verify( populator ).close( false );
    }

    private IndexPopulation addPopulator( IndexPopulator indexPopulator, int id, FlippableIndexProxy flippableIndexProxy,
            FailedIndexProxyFactory failedIndexProxyFactory )
    {
        return addPopulator(multipleIndexPopulator, indexPopulator, id, flippableIndexProxy, failedIndexProxyFactory );
    }

    private IndexPopulation addPopulator( MultipleIndexPopulator multipleIndexPopulator, IndexPopulator indexPopulator, int id,
            FlippableIndexProxy flippableIndexProxy, FailedIndexProxyFactory failedIndexProxyFactory )
    {
        return addPopulator( multipleIndexPopulator, id, IndexDescriptorFactory.forLabel( id, id ), indexPopulator,
                flippableIndexProxy, failedIndexProxyFactory );
    }

    private IndexPopulation addPopulator( MultipleIndexPopulator multipleIndexPopulator, long indexId,
            IndexDescriptor descriptor, IndexPopulator indexPopulator,
            FlippableIndexProxy flippableIndexProxy, FailedIndexProxyFactory failedIndexProxyFactory )
    {
        return multipleIndexPopulator.addPopulator( indexPopulator, indexId, descriptor,
                mock( SchemaIndexProvider.Descriptor.class ), flippableIndexProxy,
                failedIndexProxyFactory, "userIndexDescription" );
    }

    private IndexPopulation addPopulator( IndexPopulator indexPopulator, int id )
    {
        FlippableIndexProxy indexProxy = mock( FlippableIndexProxy.class );
        return addPopulator( indexPopulator, id, indexProxy, mock( FailedIndexProxyFactory.class ) );
    }
}
