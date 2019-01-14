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
package org.neo4j.kernel.api.impl.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.LuceneAllDocumentsReader;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Values;

import static java.util.Collections.emptySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asList;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class LuceneSchemaIndexIT
{

    @Inject
    private TestDirectory testDir;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private final IndexDescriptor descriptor = TestIndexDescriptorFactory.forLabel( 0, 0 );
    private final Config config = Config.defaults();

    @BeforeEach
    void before()
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", "10" );
    }

    @AfterEach
    void after()
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", "" );
    }

    @Test
    void snapshotForPartitionedIndex() throws Exception
    {
        // Given
        try ( LuceneIndexAccessor indexAccessor = createDefaultIndexAccessor() )
        {
            generateUpdates( indexAccessor, 32 );
            indexAccessor.force( IOLimiter.UNLIMITED );

            // When & Then
            List<String> singlePartitionFileTemplates = Arrays.asList( ".cfe", ".cfs", ".si", "segments_1" );
            try ( ResourceIterator<File> snapshotIterator = indexAccessor.snapshotFiles() )
            {
                List<String> indexFileNames = asFileInsidePartitionNames( snapshotIterator );

                assertTrue( indexFileNames.size() >= (singlePartitionFileTemplates.size() * 4), "Expect files from 4 partitions" );
                Map<String,Integer> templateMatches =
                        countTemplateMatches( singlePartitionFileTemplates, indexFileNames );

                for ( String fileTemplate : singlePartitionFileTemplates )
                {
                    Integer matches = templateMatches.get( fileTemplate );
                    assertTrue( matches >= 4, "Expect to see at least 4 matches for template: " + fileTemplate );
                }
            }
        }
    }

    @Test
    void snapshotForIndexWithNoCommits() throws Exception
    {
        // Given
        // A completely un-used index
        try ( LuceneIndexAccessor indexAccessor = createDefaultIndexAccessor();
              ResourceIterator<File> snapshotIterator = indexAccessor.snapshotFiles() )
        {
            assertThat( asUniqueSetOfNames( snapshotIterator ), equalTo( emptySet() ) );
        }
    }

    @Test
    void updateMultiplePartitionedIndex() throws IOException
    {
        try ( SchemaIndex index = LuceneSchemaIndexBuilder.create( descriptor, config )
                .withFileSystem( fileSystem )
                .withIndexRootFolder( testDir.directory( "partitionedIndexForUpdates" ) )
                .build() )
        {
            index.create();
            index.open();
            addDocumentToIndex( index, 45 );

            index.getIndexWriter().updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( 100 ),
                    LuceneDocumentStructure.documentRepresentingProperties( (long) 100, Values.intValue( 100 ) ) );
            index.maybeRefreshBlocking();

            long documentsInIndex = Iterators.count( index.allDocumentsReader().iterator() );
            assertEquals( 46, documentsInIndex, "Index should contain 45 added and 1 updated document." );
        }
    }

    @Test
    void createPopulateDropIndex() throws Exception
    {
        File crudOperation = testDir.directory( "indexCRUDOperation" );
        try ( SchemaIndex crudIndex = LuceneSchemaIndexBuilder.create( descriptor, config )
                .withFileSystem( fileSystem )
                .withIndexRootFolder( new File( crudOperation, "crudIndex" ) )
                .build() )
        {
            crudIndex.open();

            addDocumentToIndex( crudIndex, 1 );
            assertEquals( 1, crudIndex.getPartitions().size() );

            addDocumentToIndex( crudIndex, 21 );
            assertEquals( 3, crudIndex.getPartitions().size() );

            crudIndex.drop();

            assertFalse( crudIndex.isOpen() );
            assertEquals( 0, crudOperation.list().length );
        }
    }

    @Test
    void createFailPartitionedIndex() throws Exception
    {
        try ( SchemaIndex failedIndex = LuceneSchemaIndexBuilder.create( descriptor, config )
                .withFileSystem( fileSystem )
                .withIndexRootFolder( new File( testDir.directory( "failedIndexFolder" ), "failedIndex" ) )
                .build() )
        {
            failedIndex.open();

            addDocumentToIndex( failedIndex, 35 );
            assertEquals( 4, failedIndex.getPartitions().size() );

            failedIndex.markAsFailed( "Some failure" );
            failedIndex.flush();

            assertTrue( failedIndex.isOpen() );
            assertFalse( failedIndex.isOnline() );
        }
    }

    @Test
    void openClosePartitionedIndex() throws IOException
    {
        SchemaIndex reopenIndex = null;
        try
        {
            reopenIndex = LuceneSchemaIndexBuilder.create( descriptor, config )
                    .withFileSystem( fileSystem )
                    .withIndexRootFolder( new File( testDir.directory( "reopenIndexFolder" ), "reopenIndex" ) )
                    .build();
            reopenIndex.open();

            addDocumentToIndex( reopenIndex, 1 );

            reopenIndex.close();
            assertFalse( reopenIndex.isOpen() );

            reopenIndex.open();
            assertTrue( reopenIndex.isOpen() );

            addDocumentToIndex( reopenIndex, 10 );

            reopenIndex.close();
            assertFalse( reopenIndex.isOpen() );

            reopenIndex.open();
            assertTrue( reopenIndex.isOpen() );

            reopenIndex.close();
            reopenIndex.open();
            addDocumentToIndex( reopenIndex, 100 );

            reopenIndex.maybeRefreshBlocking();

            try ( LuceneAllDocumentsReader allDocumentsReader = reopenIndex.allDocumentsReader() )
            {
                assertEquals( 111, allDocumentsReader.maxCount(), "All documents should be visible" );
            }
        }
        finally
        {
            if ( reopenIndex != null )
            {
                reopenIndex.close();
            }
        }
    }

    private void addDocumentToIndex( SchemaIndex index, int documents ) throws IOException
    {
        for ( int i = 0; i < documents; i++ )
        {
            index.getIndexWriter().addDocument(
                    LuceneDocumentStructure.documentRepresentingProperties( (long) i, Values.intValue( i ) ) );
        }
    }

    private LuceneIndexAccessor createDefaultIndexAccessor() throws IOException
    {
        SchemaIndex index = LuceneSchemaIndexBuilder.create( descriptor, config )
                .withFileSystem( fileSystem )
                .withIndexRootFolder( testDir.directory( "testIndex" ) )
                .build();
        index.create();
        index.open();
        return new LuceneIndexAccessor( index, descriptor );
    }

    private List<String> asFileInsidePartitionNames( ResourceIterator<File> resources )
    {
        int testDirectoryPathLength = testDir.directory().getAbsolutePath().length();
        return asList( resources ).stream()
                .map( file -> file.getAbsolutePath().substring( testDirectoryPathLength ) )
                .collect( Collectors.toList() );
    }

    private void generateUpdates( LuceneIndexAccessor indexAccessor, int nodesToUpdate )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = indexAccessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( int nodeId = 0; nodeId < nodesToUpdate; nodeId++ )
            {
                updater.process( add( nodeId, nodeId ) );
            }
        }
    }

    private IndexEntryUpdate<?> add( long nodeId, Object value )
    {
        return IndexEntryUpdate.add( nodeId, descriptor.schema(), Values.of( value ) );
    }

    private static Map<String,Integer> countTemplateMatches( List<String> nameTemplates, List<String> fileNames )
    {
        Map<String,Integer> templateMatches = new HashMap<>();
        for ( String indexFileName : fileNames )
        {
            for ( String template : nameTemplates )
            {
                if ( indexFileName.endsWith( template ) )
                {
                    templateMatches.put( template, templateMatches.getOrDefault( template, 0 ) + 1 );
                }
            }
        }
        return templateMatches;
    }

    private static Set<String> asUniqueSetOfNames( ResourceIterator<File> files )
    {
        ArrayList<String> out = new ArrayList<>();
        while ( files.hasNext() )
        {
            String name = files.next().getName();
            out.add( name );
        }
        return Iterables.asUniqueSet( out );
    }
}
