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
package org.neo4j.kernel.api.impl.schema.populator;

import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.AllNodesCollector;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.change;
import static org.neo4j.kernel.api.index.IndexQueryHelper.remove;

public class UniqueDatabaseIndexPopulatorTest
{
    private final CleanupRule cleanup = new CleanupRule();
    private final TestDirectory testDir = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDir ).around( cleanup ).around( fileSystemRule );

    private static final int LABEL_ID = 1;
    private static final int PROPERTY_KEY_ID = 2;

    private final DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private static final IndexDescriptor descriptor = IndexDescriptorFactory
            .forLabel( LABEL_ID, PROPERTY_KEY_ID );

    private final PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );

    private PartitionedIndexStorage indexStorage;
    private SchemaIndex index;
    private UniqueLuceneIndexPopulator populator;
    private LabelSchemaDescriptor schemaDescriptor;

    @Before
    public void setUp() throws Exception
    {
        File folder = testDir.directory( "folder" );
        indexStorage = new PartitionedIndexStorage( directoryFactory, fileSystemRule.get(), folder, false );
        index = LuceneSchemaIndexBuilder.create( descriptor )
                .withIndexStorage( indexStorage )
                .build();
        schemaDescriptor = descriptor.schema();
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

        updater.process( change( 1, schemaDescriptor, "value1", "value2" ) );

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

        updater.process( remove( 1, schemaDescriptor, "value1" ) );
        updater.process( add( 1, schemaDescriptor, "value1" ) );

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

        updater.process( remove( 1, schemaDescriptor, "value1" ) );

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

        updater.process( change( 1, schemaDescriptor, "value1", "value2" ) );
        updater.process( change( 2, schemaDescriptor, "value2", "value1" ) );

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

        when( propertyAccessor.getPropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn( Values.of( value ) );
        when( propertyAccessor.getPropertyValue( 3, PROPERTY_KEY_ID ) ).thenReturn( Values.of( value ) );

        // when
        try
        {
            populator.verifyDeferredConstraints( propertyAccessor );

            fail( "should have thrown exception" );
        }
        // then
        catch ( IndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( Values.of( value ), conflict.getSinglePropertyValue() );
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

        when( propertyAccessor.getPropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn( Values.of( 1 ) );
        when( propertyAccessor.getPropertyValue( 3, PROPERTY_KEY_ID ) ).thenReturn( Values.of( 1 ) );

        // when
        try
        {
            populator.verifyDeferredConstraints( propertyAccessor );

            fail( "should have thrown exception" );
        }
        // then
        catch ( IndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( Values.of( 1 ), conflict.getSinglePropertyValue() );
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

        Value value = Values.of( "value1" );
        when( propertyAccessor.getPropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn( value );
        when( propertyAccessor.getPropertyValue( 3, PROPERTY_KEY_ID ) ).thenReturn( value );

        // when
        try
        {
            IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
            updater.process( add( 3, schemaDescriptor, "value1" ) );
            updater.close();

            fail( "should have thrown exception" );
        }
        // then
        catch ( IndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( value, conflict.getSinglePropertyValue() );
            assertEquals( 3, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldRejectDuplicateEntryAfterUsingPopulatingUpdater() throws Exception
    {
        // given
        populator = newPopulator();

        String valueString = "value1";
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
        updater.process( add( 1, schemaDescriptor, valueString ) );
        addUpdate( populator, 2, valueString );

        Value value = Values.of( valueString );
        when( propertyAccessor.getPropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn( value );
        when( propertyAccessor.getPropertyValue( 2, PROPERTY_KEY_ID ) ).thenReturn( value );

        // when
        try
        {
            populator.verifyDeferredConstraints( propertyAccessor );

            fail( "should have thrown exception" );
        }
        // then
        catch ( IndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( value, conflict.getSinglePropertyValue() );
            assertEquals( 2, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldNotRejectDuplicateEntryOnSameNodeIdAfterUsingPopulatingUpdater() throws Exception
    {
        // given
        populator = newPopulator();

        when( propertyAccessor.getPropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn(
                Values.of( "value1" ) );

        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );
        updater.process( add( 1, schemaDescriptor, "value1" ) );
        updater.process( change( 1, schemaDescriptor, "value1", "value1" ) );
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
        when( propertyAccessor.getPropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn( Values.of( 1000000000000000001L ) );
        when( propertyAccessor.getPropertyValue( 3, PROPERTY_KEY_ID ) ).thenReturn( Values.of( 1000000000000000002L ) );

        // Then our verification should NOT fail:
        populator.verifyDeferredConstraints( propertyAccessor );
    }

    @Test
    public void shouldCheckAllCollisionsFromPopulatorAdd() throws Exception
    {
        // given
        populator = newPopulator();

        int iterations = 228; // This value has to be high enough to stress the EntrySet implementation
        IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor );

        for ( int nodeId = 0; nodeId < iterations; nodeId++ )
        {
            updater.process( add( nodeId, schemaDescriptor, 1 ) );
            when( propertyAccessor.getPropertyValue( nodeId, PROPERTY_KEY_ID ) ).thenReturn(
                    Values.of( nodeId ) );
        }

        // ... and the actual conflicting property:
        updater.process( add( iterations, schemaDescriptor, 1 ) );
        when( propertyAccessor.getPropertyValue( iterations, PROPERTY_KEY_ID ) ).thenReturn(
                Values.of( 1 ) ); // This collision is real!!!

        // when
        try
        {
            updater.close();
            fail( "should have thrown exception" );
        }
        // then
        catch ( IndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( Values.of( 1 ), conflict.getSinglePropertyValue() );
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
            when( propertyAccessor.getPropertyValue( nodeId, PROPERTY_KEY_ID ) ).thenReturn(
                    Values.of( nodeId ) );
        }

        // ... and the actual conflicting property:
        addUpdate( populator, iterations, 1 );
        when( propertyAccessor.getPropertyValue( iterations, PROPERTY_KEY_ID ) ).thenReturn(
                Values.of( 1 ) ); // This collision is real!!!

        // when
        try
        {
            populator.verifyDeferredConstraints( propertyAccessor );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( Values.of( 1 ), conflict.getSinglePropertyValue() );
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
        executor.execute( (WorkerCommand<Void,Void>) state ->
        {
            try ( IndexUpdater updater = populator.newPopulatingUpdater( propertyAccessor ) )
            {   // Just open it and let it be closed
            }
            return null;
        } );
        // ... and where we verify deferred constraints after
        executor.execute( (WorkerCommand<Void,Void>) state ->
        {
            populator.verifyDeferredConstraints( propertyAccessor );
            return null;
        } );

        // WHEN doing more index updating after that
        // THEN it should be able to complete within a very reasonable time
        executor.execute( (WorkerCommand<Void,Void>) state ->
        {
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
        LabelSchemaDescriptor schemaDescriptor = SchemaDescriptorFactory.forLabel( 1, 1 );
        populator = newPopulator();
        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add( 1, schemaDescriptor, "foo" ),
                add( 2, schemaDescriptor, "bar" ),
                add( 3, schemaDescriptor, "baz" ),
                add( 4, schemaDescriptor, "qux" ) );

        updates.forEach( populator::includeSample );

        IndexSample sample = populator.sampleResult();

        assertEquals( new IndexSample( 4, 4, 4 ), sample );
    }

    @Test
    public void addUpdates() throws Exception
    {
        populator = newPopulator();

        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add( 1, schemaDescriptor, "aaa" ),
                add( 2, schemaDescriptor, "bbb" ),
                add( 3, schemaDescriptor, "ccc" ) );

        populator.add( updates );

        index.maybeRefreshBlocking();
        try ( IndexReader reader = index.getIndexReader() )
        {
            PrimitiveLongIterator allEntities = reader.query( IndexQuery.exists( 1 ) );
            assertArrayEquals( new long[]{1, 2, 3}, PrimitiveLongCollections.asArray( allEntities ) );
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
        IndexEntryUpdate<?> update = add( nodeId, descriptor.schema(), value );
        populator.add( asList( update ) );
    }

    private List<Long> getAllNodes( Directory directory, Object value ) throws IOException
    {
        return AllNodesCollector.getAllNodes( directory, Values.of( value ) );
    }
}
