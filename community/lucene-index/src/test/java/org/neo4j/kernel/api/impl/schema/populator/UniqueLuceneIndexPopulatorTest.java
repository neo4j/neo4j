/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.populator;

import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndex;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TargetDirectory;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.schema.AllNodesCollector.getAllNodes;
import static org.neo4j.kernel.api.properties.Property.intProperty;
import static org.neo4j.kernel.api.properties.Property.longProperty;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class UniqueLuceneIndexPopulatorTest
{
    @Rule
    public final CleanupRule cleanup = new CleanupRule();
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private static final int LABEL_ID = 1;
    private static final int PROPERTY_KEY_ID = 2;
    private static final String INDEX_IDENTIFIER = "42";

    private final DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private final IndexDescriptor descriptor = new IndexDescriptor( LABEL_ID, PROPERTY_KEY_ID );

    private final PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );

    private PartitionedIndexStorage indexStorage;
    private LuceneSchemaIndex index;
    private UniqueLuceneIndexPopulator populator;

    @Before
    public void setUp() throws Exception
    {
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File folder = testDir.directory( "folder" );
        indexStorage = new PartitionedIndexStorage( directoryFactory, fs, folder, INDEX_IDENTIFIER );
        index = LuceneSchemaIndexBuilder.create()
                .withIndexStorage( indexStorage )
                .build();
    }

    @After
    public void tearDown() throws Exception
    {
        if ( populator != null )
        {
            populator.close( false );
        }
        IOUtils.closeAll( index, directoryFactory );
    }

    @Test
    public void shouldVerifyThatThereAreNoDuplicates() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );
        addUpdate( populator, 2, "value2" );
        addUpdate( populator, 3, "value3" );

        // when
        populator.verifyDeferredConstraints( propertyAccessor );
        populator.close( true );

        // then
        assertEquals( asList( 1L ), getAllNodes( getDirectory(), "value1" ) );
        assertEquals( asList( 2L ), getAllNodes( getDirectory(), "value2" ) );
        assertEquals( asList( 3L ), getAllNodes( getDirectory(), "value3" ) );
    }

    private Directory getDirectory() throws IOException
    {
        File partitionFolder = indexStorage.getPartitionFolder( 1 );
        return indexStorage.openDirectory( partitionFolder );
    }

    @Test
    public void shouldUpdateEntryForNodeThatHasAlreadyBeenIndexed() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        updater.process( NodePropertyUpdate.change( 1, PROPERTY_KEY_ID, "value1", new long[]{}, "value2", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( Collections.EMPTY_LIST, getAllNodes( getDirectory(), "value1" ) );
        assertEquals( asList( 1L ), getAllNodes( getDirectory(), "value2" ) );
    }

    @Test
    public void shouldUpdateEntryForNodeThatHasPropertyRemovedAndThenAddedAgain() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        updater.process( NodePropertyUpdate.remove( 1, PROPERTY_KEY_ID, "value1", new long[]{} ) );
        updater.process( NodePropertyUpdate.add( 1, PROPERTY_KEY_ID, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( asList(1L), getAllNodes( getDirectory(), "value1" ) );
    }

    @Test
    public void shouldRemoveEntryForNodeThatHasAlreadyBeenIndexed() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        updater.process( NodePropertyUpdate.remove( 1, PROPERTY_KEY_ID, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( Collections.EMPTY_LIST, getAllNodes( getDirectory(), "value1" ) );
    }

    @Test
    public void shouldBeAbleToHandleSwappingOfIndexValues() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );
        addUpdate( populator, 2, "value2" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        updater.process( NodePropertyUpdate.change( 1, PROPERTY_KEY_ID, "value1", new long[]{}, "value2", new long[]{} ) );
        updater.process( NodePropertyUpdate.change( 2, PROPERTY_KEY_ID, "value2", new long[]{}, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( asList( 2L ), getAllNodes( getDirectory(), "value1" ) );
        assertEquals( asList( 1L ), getAllNodes( getDirectory(), "value2" ) );
    }

    @Test
    public void shouldFailAtVerificationStageWithAlreadyIndexedStringValue() throws Exception
    {
        // given
        populator = newPopulator();

        String value = "value1";
        addUpdate( populator, 1, value );
        addUpdate( populator, 2, "value2" );
        addUpdate( populator, 3, value );

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
        // given
        populator = newPopulator();

        addUpdate( populator, 1, 1 );
        addUpdate( populator, 2, 2 );
        addUpdate( populator, 3, 1 );

        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn( intProperty( PROPERTY_KEY_ID, 1 ) );
        when( propertyAccessor.getProperty( 3, PROPERTY_KEY_ID ) ).thenReturn( intProperty( PROPERTY_KEY_ID, 1 ) );

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
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );
        addUpdate( populator, 2, "value2" );

        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, "value1" ) );
        when( propertyAccessor.getProperty( 3, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, "value1" ) );

        // when
        try
        {
            IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
            updater.process( NodePropertyUpdate.add( 3, PROPERTY_KEY_ID, "value1", new long[]{} ) );
            updater.close();

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
        // given
        populator = newPopulator();

        String value = "value1";
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
        updater.process( NodePropertyUpdate.add( 1, PROPERTY_KEY_ID, value, new long[]{} ) );
        addUpdate( populator, 2, value );

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
        // given
        populator = newPopulator();

        when( propertyAccessor.getProperty( 1, PROPERTY_KEY_ID ) ).thenReturn(
                stringProperty( PROPERTY_KEY_ID, "value1" ) );

        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
        updater.process( NodePropertyUpdate.add( 1, PROPERTY_KEY_ID, "value1", new long[]{} ) );
        updater.process( NodePropertyUpdate.change( 1, PROPERTY_KEY_ID, "value1", new long[]{}, "value1", new long[]{} ) );
        updater.close();
        addUpdate( populator, 2, "value2" );
        addUpdate( populator, 3, "value3" );

        // when
        populator.verifyDeferredConstraints( propertyAccessor );
        populator.close( true );

        // then
        assertEquals( asList( 1L ), getAllNodes( getDirectory(), "value1" ) );
        assertEquals( asList( 2L ), getAllNodes( getDirectory(), "value2" ) );
        assertEquals( asList( 3L ), getAllNodes( getDirectory(), "value3" ) );
    }

    @Test
    public void shouldNotRejectIndexCollisionsCausedByPrecisionLossAsDuplicates() throws Exception
    {
        // given
        populator = newPopulator();

        // Given we have a collision in our index...
        addUpdate( populator, 1, 1000000000000000001L );
        addUpdate( populator, 2, 2 );
        addUpdate( populator, 3, 1000000000000000001L );

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
        // given
        populator = newPopulator();

        int iterations = 228; // This value has to be high enough to stress the EntrySet implementation
        long[] labels = new long[0];
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        for ( int nodeId = 0; nodeId < iterations; nodeId++ )
        {
            updater.process( NodePropertyUpdate.add( nodeId, PROPERTY_KEY_ID, 1, labels ) );
            when( propertyAccessor.getProperty( nodeId, PROPERTY_KEY_ID ) ).thenReturn(
                    intProperty( PROPERTY_KEY_ID, nodeId ) );
        }

        // ... and the actual conflicting property:
        updater.process( NodePropertyUpdate.add( iterations, PROPERTY_KEY_ID, 1, labels ) );
        when( propertyAccessor.getProperty( iterations, PROPERTY_KEY_ID ) ).thenReturn(
                intProperty( PROPERTY_KEY_ID, 1 ) ); // This collision is real!!!

        // when
        try
        {
            updater.close();
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
        // given
        populator = newPopulator();

        int iterations = 228; // This value has to be high enough to stress the EntrySet implementation

        for ( int nodeId = 0; nodeId < iterations; nodeId++ )
        {
            addUpdate( populator, nodeId, 1 );
            when( propertyAccessor.getProperty( nodeId, PROPERTY_KEY_ID ) ).thenReturn(
                    intProperty( PROPERTY_KEY_ID, nodeId ) );
        }

        // ... and the actual conflicting property:
        addUpdate( populator, iterations, 1 );
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
        // given
        populator = newPopulator();

        /*
         * This test was created due to a problem in closing an index updater after deferred constraints
         * had been verified, where it got stuck in a busy loop in ReferenceManager#acquire.
         */

        // GIVEN an index updater that we close
        OtherThreadExecutor<Void> executor = cleanup.add( new OtherThreadExecutor<>( "Deferred", null ) );
        executor.execute( (WorkerCommand<Void,Void>) state -> {
            try ( IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor ) )
            {   // Just open it and let it be closed
            }
            return null;
        } );
        // ... and where we verify deferred constraints after
        executor.execute( (WorkerCommand<Void,Void>) state -> {
            populator.verifyDeferredConstraints( propertyAccessor );
            return null;
        } );

        // WHEN doing more index updating after that
        // THEN it should be able to complete within a very reasonable time
        executor.execute( (WorkerCommand<Void,Void>) state -> {
            try ( IndexUpdater secondUpdater = populator.newPopulatingUpdater( propertyAccessor ) )
            {   // Just open it and let it be closed
            }
            return null;
        }, 5, SECONDS );
    }

    @Test
    public void sampleEmptyIndex() throws Exception
    {
        populator = newPopulator();

        IndexSample sample = populator.sampleResult();

        assertEquals( new IndexSample(), sample );
    }

    @Test
    public void sampleIncludedUpdates() throws Exception
    {
        populator = newPopulator();
        List<NodePropertyUpdate> updates = Arrays.asList(
                NodePropertyUpdate.add( 1, 1, "foo", new long[]{1} ),
                NodePropertyUpdate.add( 2, 1, "bar", new long[]{1} ),
                NodePropertyUpdate.add( 3, 1, "baz", new long[]{1} ),
                NodePropertyUpdate.add( 4, 1, "qux", new long[]{1} ) );

        updates.forEach( populator::includeSample );

        IndexSample sample = populator.sampleResult();

        assertEquals( new IndexSample( 4, 4, 4 ), sample );
    }

    @Test
    public void addUpdates() throws Exception
    {
        populator = newPopulator();

        List<NodePropertyUpdate> updates = Arrays.asList(
                NodePropertyUpdate.add( 1, 1, "aaa", new long[]{1} ),
                NodePropertyUpdate.add( 2, 1, "bbb", new long[]{1} ),
                NodePropertyUpdate.add( 3, 1, "ccc", new long[]{1} ) );

        populator.add( updates );

        index.maybeRefreshBlocking();
        try ( IndexReader reader = index.getIndexReader() )
        {
            assertArrayEquals( new long[]{1, 2, 3}, PrimitiveLongCollections.asArray( reader.scan() ) );
        }
    }

    private UniqueLuceneIndexPopulator newPopulator() throws IOException
    {
        UniqueLuceneIndexPopulator populator = new UniqueLuceneIndexPopulator( index, descriptor );
        populator.create();
        return populator;
    }

    private static void addUpdate( UniqueLuceneIndexPopulator populator, long nodeId, Object value )
            throws IOException, IndexEntryConflictException
    {
        NodePropertyUpdate update = NodePropertyUpdate.add( nodeId, 0, value, new long[]{0} );
        populator.add( Collections.singletonList( update ) );
    }
}
