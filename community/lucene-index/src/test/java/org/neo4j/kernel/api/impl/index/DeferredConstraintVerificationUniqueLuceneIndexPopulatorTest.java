/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.PreparedIndexUpdates;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.util.FailureStorage;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.index.AllNodesCollector.getAllNodes;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.tracking;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.change;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.remove;
import static org.neo4j.kernel.api.properties.Property.intProperty;
import static org.neo4j.kernel.api.properties.Property.longProperty;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class DeferredConstraintVerificationUniqueLuceneIndexPopulatorTest
{
    @Test
    public void shouldVerifyThatThereAreNoDuplicates() throws Exception
    {
        populator.add( 1, "value1" );
        populator.add( 2, "value2" );
        populator.add( 3, "value3" );

        // when
        populator.verifyDeferredConstraints( propertyAccessor );
        populator.close( true );

        // then
        assertEquals( asList( 1l ), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
        assertEquals( asList( 2l ), getAllNodes( directoryFactory, indexDirectory, "value2" ) );
        assertEquals( asList( 3l ), getAllNodes( directoryFactory, indexDirectory, "value3" ) );
    }

    @Test
    public void shouldUpdateEntryForNodeThatHasAlreadyBeenIndexed() throws Exception
    {
        populator.add( 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        processUpdates( updater, change( 1, PROPERTY_KEY_ID, "value1", new long[]{}, "value2", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( Collections.<Long>emptyList(), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
        assertEquals( asList( 1l ), getAllNodes( directoryFactory, indexDirectory, "value2" ) );
    }

    @Test
    public void shouldUpdateEntryForNodeThatHasPropertyRemovedAndThenAddedAgain() throws Exception
    {
        populator.add( 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        processUpdates( updater,
                remove( 1, PROPERTY_KEY_ID, "value1", new long[]{} ),
                add( 1, PROPERTY_KEY_ID, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( asList(1l), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
    }

    @Test
    public void shouldRemoveEntryForNodeThatHasAlreadyBeenIndexed() throws Exception
    {
        populator.add( 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        processUpdates( updater, remove( 1, PROPERTY_KEY_ID, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( Collections.EMPTY_LIST, getAllNodes( directoryFactory, indexDirectory, "value1" ) );
    }

    @Test
    public void shouldBeAbleToHandleSwappingOfIndexValues() throws Exception
    {
        populator.add( 1, "value1" );
        populator.add( 2, "value2" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        processUpdates( updater,
                change( 1, PROPERTY_KEY_ID, "value1", new long[]{}, "value2", new long[]{} ),
                change( 2, PROPERTY_KEY_ID, "value2", new long[]{}, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( asList( 2l ), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
        assertEquals( asList( 1l ), getAllNodes( directoryFactory, indexDirectory, "value2" ) );
    }

    @Test
    public void shouldFailAtVerificationStageWithAlreadyIndexedStringValue() throws Exception
    {
        String value = "value1";
        populator.add( 1, value );
        populator.add( 2, "value2" );
        populator.add( 3, value );

        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, value ) );
        when( propertyAccessor.getProperty( 3, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, value ) );

        // when
        try
        {
            populator.verifyDeferredConstraints( propertyAccessor );

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( value, conflict.getPropertyValue() );
            assertEquals( 3, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldFailAtVerificationStageWithAlreadyIndexedNumberValue() throws Exception
    {
        populator.add( 1, 1 );
        populator.add( 2, 2 );
        populator.add( 3, 1 );


        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn(
                intProperty( PROPERTY_KEY_ID, 1 ) );
        when( propertyAccessor.getProperty( 3, PROPERTY_KEY_ID ) ).thenReturn(
                intProperty( PROPERTY_KEY_ID, 1 ) );

        // when
        try
        {
            populator.verifyDeferredConstraints( propertyAccessor );

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( 1, conflict.getPropertyValue() );
            assertEquals( 3, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldRejectDuplicateEntryWhenUsingPopulatingUpdater() throws Exception
    {
        populator.add( 1, "value1" );
        populator.add( 2, "value2" );

        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, "value1" ) );
        when( propertyAccessor.getProperty( 3, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, "value1" ) );

        // when
        try
        {
            IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
            processUpdates( updater, add( 3, PROPERTY_KEY_ID, "value1", new long[]{} ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 3, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldRejectDuplicateEntryAfterUsingPopulatingUpdater() throws Exception
    {
        String value = "value1";
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
        processUpdates( updater, add( 1, PROPERTY_KEY_ID, value, new long[]{} ) );
        populator.add( 2, value );

        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, value ) );
        when( propertyAccessor.getProperty( 2, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, value ) );

        // when
        try
        {
            populator.verifyDeferredConstraints( propertyAccessor );

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( value, conflict.getPropertyValue() );
            assertEquals( 2, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldNotRejectDuplicateEntryOnSameNodeIdAfterUsingPopulatingUpdater() throws Exception
    {
        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, "value1" ) );

        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
        processUpdates( updater,
                add( 1, PROPERTY_KEY_ID, "value1", new long[]{} ),
                change( 1, PROPERTY_KEY_ID, "value1", new long[]{}, "value1", new long[]{} ) );

        populator.add( 2, "value2" );
        populator.add( 3, "value3" );

        // when
        populator.verifyDeferredConstraints( propertyAccessor );
        populator.close( true );

        // then
        assertEquals( asList( 1l ), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
        assertEquals( asList( 2l ), getAllNodes( directoryFactory, indexDirectory, "value2" ) );
        assertEquals( asList( 3l ), getAllNodes( directoryFactory, indexDirectory, "value3" ) );
    }

    @Test
    public void shouldNotRejectIndexCollisionsCausedByPrecisionLossAsDuplicates() throws Exception
    {
        // Given we have a collision in our index...
        populator.add( 1, 1000000000000000001L );
        populator.add( 2, 2 );
        populator.add( 3, 1000000000000000001L );

        // ... but the actual data in the store does not collide
        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn(
                longProperty( PROPERTY_KEY_ID, 1000000000000000001L ) );
        when( propertyAccessor.getProperty( 3, PROPERTY_KEY_ID ) ).thenReturn(
                longProperty( PROPERTY_KEY_ID, 1000000000000000002L ) );

        // Then our verification should NOT fail:
        populator.verifyDeferredConstraints( propertyAccessor );
    }

    @Test
    public void shouldCheckAllCollisionsFromPopulatorAdd() throws Exception
    {
        int iterations = 228; // This value has to be high enough to stress the EntrySet implementation
        long[] labels = new long[0];
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        for ( int nodeId = 0; nodeId < iterations; nodeId++ )
        {
            processUpdates( updater, add( nodeId, PROPERTY_KEY_ID, 1, labels ) );
            when( propertyAccessor.getProperty( nodeId, PROPERTY_KEY_ID ) ).thenReturn(
                    intProperty( PROPERTY_KEY_ID, nodeId ) );
        }

        // ... and the actual conflicting property:
        PreparedIndexUpdates updates = updater.prepare( asList( add( iterations, PROPERTY_KEY_ID, 1, labels ) ) );
        when( propertyAccessor.getProperty( iterations, PROPERTY_KEY_ID ) ).thenReturn(
                intProperty( PROPERTY_KEY_ID, 1 ) ); // This collision is real!!!

        // when
        try
        {
            updates.commit();
            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( 1, conflict.getPropertyValue() );
            assertEquals( iterations, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldCheckAllCollisionsFromUpdaterClose() throws Exception
    {
        int iterations = 228; // This value has to be high enough to stress the EntrySet implementation

        for ( int nodeId = 0; nodeId < iterations; nodeId++ )
        {
            populator.add( nodeId, 1 );
            when( propertyAccessor.getProperty( nodeId, PROPERTY_KEY_ID ) ).thenReturn(
                    intProperty( PROPERTY_KEY_ID, nodeId ) );
        }

        // ... and the actual conflicting property:
        populator.add( iterations, 1 );
        when( propertyAccessor.getProperty( iterations, PROPERTY_KEY_ID ) ).thenReturn(
                intProperty( PROPERTY_KEY_ID, 1 ) ); // This collision is real!!!

        // when
        try
        {
            populator.verifyDeferredConstraints( propertyAccessor );
            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( 1, conflict.getPropertyValue() );
            assertEquals( iterations, conflict.getAddedNodeId() );
        }
    }
    
    @Test
    public void shouldReleaseSearcherProperlyAfterVerifyingDeferredConstraints() throws Exception
    {
        /*
         * This test was created due to a problem in closing an index updater after deferred constraints
         * had been verified, where it got stuck in a busy loop in ReferenceManager#acquire.
         */
        
        // GIVEN an index updater that we close
        OtherThreadExecutor<Void> executor = cleanup.add( new OtherThreadExecutor<Void>( "Deferred", null ) );
        executor.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
                updater.prepare( asList( add( 1, 1, "foo", new long[]{1} ) ) ).commit();
                return null;
            }
        } );
        // ... and where we verify deferred constraints after
        executor.execute( new WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                populator.verifyDeferredConstraints( propertyAccessor );
                return null;
            }
        } );
        
        // WHEN doing more index updating after that
        // THEN it should be able to complete within a very reasonable time
        executor.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                IndexUpdater secondUpdater = populator.newPopulatingUpdater( propertyAccessor );
                secondUpdater.prepare( asList( add( 2, 2, "bar", new long[]{2} ) ) ).commit();
                return null;
            }
        }, 5, SECONDS );
    }

    @Test
    public void preparedIndexUpdatesShouldDoActualModificationsOnCommit() throws Exception
    {
        // Given
        long nodeToAddId = 1;
        long nodeToRemoveId = 1;

        final AtomicReference<LuceneIndexWriter> writer = new AtomicReference<>();
        LuceneIndexWriterFactory writerFactory = mock( LuceneIndexWriterFactory.class );
        when( writerFactory.create( any( Directory.class ) ) ).then( new Answer<LuceneIndexWriter>()
        {
            @Override
            public LuceneIndexWriter answer( InvocationOnMock invocation ) throws Throwable
            {
                Directory dir = (Directory) invocation.getArguments()[0];
                LuceneIndexWriter luceneIndexWriter = spy( tracking().create( dir ) );
                writer.set( luceneIndexWriter );
                return luceneIndexWriter;
            }
        } );

        initPopulator( writerFactory );

        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        PreparedIndexUpdates updates = updater.prepare( asList(
                add( nodeToAddId, PROPERTY_KEY_ID, "foo", new long[]{LABEL_ID} ),
                remove( nodeToRemoveId, PROPERTY_KEY_ID, "bar", new long[]{LABEL_ID} ) ) );

        reset( writer.get() );

        // When
        updates.commit();

        // Then
        verify( writer.get() ).updateDocument( eq( documentLogic.newQueryForChangeOrRemove( nodeToAddId ) ),
                any( Document.class ) );
        verify( writer.get() ).deleteDocuments( documentLogic.newQueryForChangeOrRemove( nodeToRemoveId ) );
    }

    private static void processUpdates( IndexUpdater updater, NodePropertyUpdate... updates )
            throws IOException, IndexEntryConflictException
    {
        List<NodePropertyUpdate> listOfUpdates = new ArrayList<>();
        Collections.addAll( listOfUpdates, updates );
        PreparedIndexUpdates preparedUpdates = updater.prepare( listOfUpdates );
        preparedUpdates.commit();
    }

    private static final int LABEL_ID = 1;
    private static final int PROPERTY_KEY_ID = 2;

    private final FailureStorage failureStorage = mock( FailureStorage.class );
    private final long indexId = 1;
    private final DirectoryFactory.InMemoryDirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private final IndexDescriptor descriptor = new IndexDescriptor( LABEL_ID, PROPERTY_KEY_ID );

    private File indexDirectory;
    private LuceneDocumentStructure documentLogic;
    private PropertyAccessor propertyAccessor;
    private DeferredConstraintVerificationUniqueLuceneIndexPopulator populator;
    public final @Rule CleanupRule cleanup = new CleanupRule();

    private void initPopulator( LuceneIndexWriterFactory indexWriterFactory ) throws IOException
    {
        if ( populator != null )
        {
            populator.close( false );
        }
        populator = new DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, indexWriterFactory,
                directoryFactory, indexDirectory,
                failureStorage, indexId, descriptor );

        populator.create();
    }

    @Before
    public void setUp() throws Exception
    {
        indexDirectory = new File( "target/whatever" );
        documentLogic = new LuceneDocumentStructure();
        propertyAccessor = mock( PropertyAccessor.class, RETURNS_MOCKS );
        initPopulator( tracking() );
    }
}
