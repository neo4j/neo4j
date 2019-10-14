/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.AllNodesCollector;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.change;
import static org.neo4j.kernel.api.index.IndexQueryHelper.remove;

@TestDirectoryExtension
class UniqueDatabaseIndexPopulatorTest
{
    private static final int LABEL_ID = 1;
    private static final int PROPERTY_KEY_ID = 2;
    private static final IndexDescriptor descriptor = IndexPrototype.forSchema( forLabel( LABEL_ID, PROPERTY_KEY_ID ) ).withName( "index" ).materialise( 0 );

    @Inject
    private TestDirectory testDir;
    @Inject
    private FileSystemAbstraction fs;

    private final DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private final NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );

    private PartitionedIndexStorage indexStorage;
    private SchemaIndex index;
    private UniqueLuceneIndexPopulator populator;
    private SchemaDescriptor schemaDescriptor;

    @BeforeEach
    void setUp()
    {
        File folder = testDir.directory( "folder" );
        indexStorage = new PartitionedIndexStorage( directoryFactory, fs, folder );
        index = LuceneSchemaIndexBuilder.create( descriptor, Config.defaults() )
                .withIndexStorage( indexStorage )
                .build();
        schemaDescriptor = descriptor.schema();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if ( populator != null )
        {
            populator.close( false );
        }
        IOUtils.closeAll( index, directoryFactory );
    }

    @Test
    void shouldVerifyThatThereAreNoDuplicates() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );
        addUpdate( populator, 2, "value2" );
        addUpdate( populator, 3, "value3" );

        // when
        populator.verifyDeferredConstraints( nodePropertyAccessor );
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
    void shouldUpdateEntryForNodeThatHasAlreadyBeenIndexed() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor );

        updater.process( change( 1, schemaDescriptor, "value1", "value2" ) );

        populator.close( true );

        // then
        assertEquals( Collections.EMPTY_LIST, getAllNodes( getDirectory(), "value1" ) );
        assertEquals( asList( 1L ), getAllNodes( getDirectory(), "value2" ) );
    }

    @Test
    void shouldUpdateEntryForNodeThatHasPropertyRemovedAndThenAddedAgain() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor );

        updater.process( remove( 1, schemaDescriptor, "value1" ) );
        updater.process( add( 1, schemaDescriptor, "value1" ) );

        populator.close( true );

        // then
        assertEquals( asList(1L), getAllNodes( getDirectory(), "value1" ) );
    }

    @Test
    void shouldRemoveEntryForNodeThatHasAlreadyBeenIndexed() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor );

        updater.process( remove( 1, schemaDescriptor, "value1" ) );

        populator.close( true );

        // then
        assertEquals( Collections.EMPTY_LIST, getAllNodes( getDirectory(), "value1" ) );
    }

    @Test
    void shouldBeAbleToHandleSwappingOfIndexValues() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );
        addUpdate( populator, 2, "value2" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor );

        updater.process( change( 1, schemaDescriptor, "value1", "value2" ) );
        updater.process( change( 2, schemaDescriptor, "value2", "value1" ) );

        populator.close( true );

        // then
        assertEquals( asList( 2L ), getAllNodes( getDirectory(), "value1" ) );
        assertEquals( asList( 1L ), getAllNodes( getDirectory(), "value2" ) );
    }

    @Test
    void shouldFailAtVerificationStageWithAlreadyIndexedStringValue() throws Exception
    {
        // given
        populator = newPopulator();

        String value = "value1";
        addUpdate( populator, 1, value );
        addUpdate( populator, 2, "value2" );
        addUpdate( populator, 3, value );

        when( nodePropertyAccessor.getNodePropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn( Values.of( value ) );
        when( nodePropertyAccessor.getNodePropertyValue( 3, PROPERTY_KEY_ID ) ).thenReturn( Values.of( value ) );

        var conflict = assertThrows( IndexEntryConflictException.class, () -> populator.verifyDeferredConstraints( nodePropertyAccessor ) );
        assertEquals( 1, conflict.getExistingNodeId() );
        assertEquals( Values.of( value ), conflict.getSinglePropertyValue() );
        assertEquals( 3, conflict.getAddedNodeId() );
    }

    @Test
    void shouldRejectDuplicateEntryWhenUsingPopulatingUpdater() throws Exception
    {
        // given
        populator = newPopulator();

        addUpdate( populator, 1, "value1" );
        addUpdate( populator, 2, "value2" );

        Value value = Values.of( "value1" );
        when( nodePropertyAccessor.getNodePropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn( value );
        when( nodePropertyAccessor.getNodePropertyValue( 3, PROPERTY_KEY_ID ) ).thenReturn( value );

        // when
        var conflict = assertThrows( IndexEntryConflictException.class, () ->
        {
            IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor );
            updater.process( add( 3, schemaDescriptor, "value1" ) );
            updater.close();
        } );
        assertEquals( 1, conflict.getExistingNodeId() );
        assertEquals( value, conflict.getSinglePropertyValue() );
        assertEquals( 3, conflict.getAddedNodeId() );
    }

    @Test
    void shouldRejectDuplicateEntryAfterUsingPopulatingUpdater() throws Exception
    {
        // given
        populator = newPopulator();

        String valueString = "value1";
        IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor );
        updater.process( add( 1, schemaDescriptor, valueString ) );
        addUpdate( populator, 2, valueString );

        Value value = Values.of( valueString );
        when( nodePropertyAccessor.getNodePropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn( value );
        when( nodePropertyAccessor.getNodePropertyValue( 2, PROPERTY_KEY_ID ) ).thenReturn( value );

        // when
        var conflict = assertThrows( IndexEntryConflictException.class, () -> populator.verifyDeferredConstraints( nodePropertyAccessor ) );
        assertEquals( 1, conflict.getExistingNodeId() );
        assertEquals( value, conflict.getSinglePropertyValue() );
        assertEquals( 2, conflict.getAddedNodeId() );
    }

    @Test
    void shouldNotRejectDuplicateEntryOnSameNodeIdAfterUsingPopulatingUpdater() throws Exception
    {
        // given
        populator = newPopulator();

        when( nodePropertyAccessor.getNodePropertyValue( 1, PROPERTY_KEY_ID ) ).thenReturn(
                Values.of( "value1" ) );

        IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor );
        updater.process( add( 1, schemaDescriptor, "value1" ) );
        updater.process( change( 1, schemaDescriptor, "value1", "value1" ) );
        updater.close();
        addUpdate( populator, 2, "value2" );
        addUpdate( populator, 3, "value3" );

        // when
        populator.verifyDeferredConstraints( nodePropertyAccessor );
        populator.close( true );

        // then
        assertEquals( asList( 1L ), getAllNodes( getDirectory(), "value1" ) );
        assertEquals( asList( 2L ), getAllNodes( getDirectory(), "value2" ) );
        assertEquals( asList( 3L ), getAllNodes( getDirectory(), "value3" ) );
    }

    @Test
    void shouldCheckAllCollisionsFromPopulatorAdd() throws Exception
    {
        // given
        populator = newPopulator();

        int iterations = 228; // This value has to be high enough to stress the EntrySet implementation
        IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor );

        for ( int nodeId = 0; nodeId < iterations; nodeId++ )
        {
            updater.process( add( nodeId, schemaDescriptor, "1" ) );
            when( nodePropertyAccessor.getNodePropertyValue( nodeId, PROPERTY_KEY_ID ) ).thenReturn(
                    Values.of( nodeId ) );
        }

        // ... and the actual conflicting property:
        updater.process( add( iterations, schemaDescriptor, "1" ) );
        when( nodePropertyAccessor.getNodePropertyValue( iterations, PROPERTY_KEY_ID ) ).thenReturn(
                Values.of( 1 ) ); // This collision is real!!!

        // when
        var conflict = assertThrows( IndexEntryConflictException.class, updater::close );
        assertEquals( 1, conflict.getExistingNodeId() );
        assertEquals( Values.of( 1 ), conflict.getSinglePropertyValue() );
        assertEquals( iterations, conflict.getAddedNodeId() );
    }

    @Test
    void shouldCheckAllCollisionsFromUpdaterClose() throws Exception
    {
        // given
        populator = newPopulator();

        int iterations = 228; // This value has to be high enough to stress the EntrySet implementation

        for ( int nodeId = 0; nodeId < iterations; nodeId++ )
        {
            addUpdate( populator, nodeId, "1" );
            when( nodePropertyAccessor.getNodePropertyValue( nodeId, PROPERTY_KEY_ID ) ).thenReturn(
                    Values.of( nodeId ) );
        }

        // ... and the actual conflicting property:
        addUpdate( populator, iterations, "1" );
        when( nodePropertyAccessor.getNodePropertyValue( iterations, PROPERTY_KEY_ID ) ).thenReturn(
                Values.of( 1 ) ); // This collision is real!!!

        // when
        var conflict = assertThrows( IndexEntryConflictException.class, () -> populator.verifyDeferredConstraints( nodePropertyAccessor ) );
        assertEquals( 1, conflict.getExistingNodeId() );
        assertEquals( Values.of( 1 ), conflict.getSinglePropertyValue() );
        assertEquals( iterations, conflict.getAddedNodeId() );
    }

    @Test
    void shouldReleaseSearcherProperlyAfterVerifyingDeferredConstraints() throws Exception
    {
        // given
        populator = newPopulator();

        /*
         * This test was created due to a problem in closing an index updater after deferred constraints
         * had been verified, where it got stuck in a busy loop in ReferenceManager#acquire.
         */

        // GIVEN an index updater that we close
        var executor = Executors.newCachedThreadPool();
        executor.submit( () ->
        {
            try ( IndexUpdater updater = populator.newPopulatingUpdater( nodePropertyAccessor ) )
            {   // Just open it and let it be closed
            }
            catch ( IndexEntryConflictException e )
            {
                throw new RuntimeException( e );
            }
        } );
        // ... and where we verify deferred constraints after
        executor.submit( () ->
        {
            try
            {
                populator.verifyDeferredConstraints( nodePropertyAccessor );
            }
            catch ( IndexEntryConflictException e )
            {
                throw new RuntimeException( e );
            }
        } );

        // WHEN doing more index updating after that
        // THEN it should be able to complete within a very reasonable time
        executor.submit( () ->
        {
            try ( IndexUpdater secondUpdater = populator.newPopulatingUpdater( nodePropertyAccessor ) )
            {   // Just open it and let it be closed
            }
            return null;
        } ).get( 5, SECONDS );
    }

    @Test
    void sampleEmptyIndex() throws Exception
    {
        populator = newPopulator();

        IndexSample sample = populator.sampleResult();

        assertEquals( new IndexSample(), sample );
    }

    @Test
    void sampleIncludedUpdates() throws Exception
    {
        LabelSchemaDescriptor schemaDescriptor = forLabel( 1, 1 );
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
    void addUpdates() throws Exception
    {
        populator = newPopulator();

        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add( 1, schemaDescriptor, "aaa" ),
                add( 2, schemaDescriptor, "bbb" ),
                add( 3, schemaDescriptor, "ccc" ) );

        populator.add( updates );

        index.maybeRefreshBlocking();
        try ( IndexReader reader = index.getIndexReader();
              NodeValueIterator allEntities = new NodeValueIterator() )
        {
            reader.query( NULL_CONTEXT, allEntities, IndexOrder.NONE, false, IndexQuery.exists( 1 ) );
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
            throws IOException
    {
        IndexEntryUpdate<?> update = add( nodeId, descriptor.schema(), value );
        populator.add( asList( update ) );
    }

    private List<Long> getAllNodes( Directory directory, Object value ) throws IOException
    {
        return AllNodesCollector.getAllNodes( directory, Values.of( value ) );
    }
}
