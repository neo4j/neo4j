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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Long.parseLong;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.heapBufferFactory;

@TestDirectoryExtension
class LuceneSchemaIndexPopulatorTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDir;

    private LuceneIndexProvider provider;
    private Directory directory;
    private IndexPopulator indexPopulator;
    private IndexReader reader;
    private IndexSearcher searcher;
    private static final int propertyKeyId = 666;
    private IndexDescriptor index;
    private NodePropertyAccessor propertyAccessor;

    @BeforeEach
    void before()
    {
        directory = new RAMDirectory();
        DirectoryFactory directoryFactory = new DirectoryFactory.Single(
                new DirectoryFactory.UncloseableDirectory( directory ) );
        provider = new LuceneIndexProvider( fs, directoryFactory, directoriesByProvider( testDir.directory( "folder" ) ),
                IndexProvider.Monitor.EMPTY, Config.defaults(), OperationalMode.SINGLE );
        propertyAccessor = mock( NodePropertyAccessor.class );
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
        index = IndexPrototype.forSchema( forLabel( 42, propertyKeyId ), provider.getProviderDescriptor() ).withName( "index" ).materialise( 0 );
        indexPopulator = provider.getPopulator( index, samplingConfig, heapBufferFactory( 1024 ) );
        indexPopulator.create();
    }

    @AfterEach
    void after() throws Exception
    {
        if ( reader != null )
        {
            reader.close();
        }
        directory.close();
    }

    @Test
    void addingValuesShouldPersistThem() throws Exception
    {
        // WHEN
        addUpdate( indexPopulator, 1, "First" );
        addUpdate( indexPopulator, 2, "Second" );
        addUpdate( indexPopulator, 3, "(byte) 1" );
        addUpdate( indexPopulator, 4, "(short) 2" );
        addUpdate( indexPopulator, 5, "3" );
        addUpdate( indexPopulator, 6, "4L" );
        addUpdate( indexPopulator, 7, "5F" );
        addUpdate( indexPopulator, 8, "6D" );

        // THEN
        assertIndexedValues(
                hit( "First", 1 ),
                hit( "Second", 2 ),
                hit( "(byte) 1", 3 ),
                hit( "(short) 2", 4 ),
                hit( "3", 5 ),
                hit( "4L", 6 ),
                hit( "5F", 7 ),
                hit( "6D", 8 ) );
    }

    @Test
    void multipleEqualValues() throws Exception
    {
        // WHEN
        addUpdate( indexPopulator, 1, "value" );
        addUpdate( indexPopulator, 2, "value" );
        addUpdate( indexPopulator, 3, "value" );

        // THEN
        assertIndexedValues(
                hit( "value", 1L, 2L, 3L ) );
    }

    @Test
    void multipleEqualValuesWithUpdateThatRemovesOne() throws Exception
    {
        // WHEN
        addUpdate( indexPopulator, 1, "value" );
        addUpdate( indexPopulator, 2, "value" );
        addUpdate( indexPopulator, 3, "value" );
        updatePopulator( indexPopulator, singletonList( remove( 2, "value" ) ), propertyAccessor );

        // THEN
        assertIndexedValues(
                hit( "value", 1L, 3L ) );
    }

    @Test
    void changeUpdatesInterleavedWithAdds() throws Exception
    {
        // WHEN
        addUpdate( indexPopulator, 1, "1" );
        addUpdate( indexPopulator, 2, "2" );
        updatePopulator( indexPopulator, singletonList( change( 1, "1", "1a" ) ), propertyAccessor );
        addUpdate( indexPopulator, 3, "3" );

        // THEN
        assertIndexedValues(
                no( "1" ),
                hit( "1a", 1 ),
                hit( "2", 2 ),
                hit( "3", 3 ) );
    }

    @Test
    void addUpdatesInterleavedWithAdds() throws Exception
    {
        // WHEN
        addUpdate( indexPopulator, 1, "1" );
        addUpdate( indexPopulator, 2, "2" );
        updatePopulator( indexPopulator, asList( remove( 1, "1" ), add( 1, "1a" ) ), propertyAccessor );
        addUpdate( indexPopulator, 3, "3" );

        // THEN
        assertIndexedValues(
                hit( "1a", 1 ),
                hit( "2", 2 ),
                hit( "3", 3 ),
                no( "1" ) );
    }

    @Test
    void removeUpdatesInterleavedWithAdds() throws Exception
    {
        // WHEN
        addUpdate( indexPopulator, 1, "1" );
        addUpdate( indexPopulator, 2, "2" );
        updatePopulator( indexPopulator, singletonList( remove( 2, "2" ) ), propertyAccessor );
        addUpdate( indexPopulator, 3, "3" );

        // THEN
        assertIndexedValues(
                hit( "1", 1 ),
                no( "2" ),
                hit( "3", 3 ) );
    }

    @Test
    void multipleInterleaves() throws Exception
    {
        // WHEN
        addUpdate( indexPopulator, 1, "1" );
        addUpdate( indexPopulator, 2, "2" );
        updatePopulator( indexPopulator, asList( change( 1, "1", "1a" ), change( 2, "2", "2a" ) ), propertyAccessor );
        addUpdate( indexPopulator, 3, "3" );
        addUpdate( indexPopulator, 4, "4" );
        updatePopulator( indexPopulator, asList( change( 1, "1a", "1b" ), change( 4, "4", "4a" ) ), propertyAccessor );

        // THEN
        assertIndexedValues(
                no( "1" ),
                no( "1a" ),
                hit( "1b", 1 ),
                no( "2" ),
                hit( "2a", 2 ),
                hit( "3", 3 ),
                no( "4" ),
                hit( "4a", 4 ) );
    }

    private Hit hit( Object value, Long... nodeIds )
    {
        return new Hit( value, nodeIds );
    }

    private Hit hit( Object value, long nodeId )
    {
        return new Hit( value, nodeId );
    }

    private Hit no( Object value )
    {
        return new Hit( value );
    }

    private static class Hit
    {
        private final Value value;
        private final Long[] nodeIds;

        Hit( Object value, Long... nodeIds )
        {
            this.value = Values.of( value );
            this.nodeIds = nodeIds;
        }
    }

    private IndexEntryUpdate<?> add( long nodeId, Object value )
    {
        return IndexQueryHelper.add( nodeId, index.schema(), value );
    }

    private IndexEntryUpdate<?> change( long nodeId, Object valueBefore, Object valueAfter )
    {
        return IndexQueryHelper.change( nodeId, index.schema(), valueBefore, valueAfter );
    }

    private IndexEntryUpdate<?> remove( long nodeId, Object removedValue )
    {
        return IndexQueryHelper.remove( nodeId, index.schema(), removedValue );
    }

    private void assertIndexedValues( Hit... expectedHits ) throws IOException
    {
        switchToVerification();

        for ( Hit hit : expectedHits )
        {
            TopDocs hits = searcher.search( LuceneDocumentStructure.newSeekQuery( hit.value ), 10 );
            assertEquals( TotalHits.Relation.EQUAL_TO, hits.totalHits.relation );
            assertEquals( hit.nodeIds.length, hits.totalHits.value, "Unexpected number of index results from " + hit.value );
            Set<Long> foundNodeIds = new HashSet<>();
            for ( int i = 0; i < hits.totalHits.value; i++ )
            {
                Document document = searcher.doc( hits.scoreDocs[i].doc );
                foundNodeIds.add( parseLong( document.get( "id" ) ) );
            }
            assertEquals( asSet( hit.nodeIds ), foundNodeIds );
        }
    }

    private void switchToVerification() throws IOException
    {
        indexPopulator.close( true );
        assertEquals( InternalIndexState.ONLINE, provider.getInitialState( index ) );
        reader = DirectoryReader.open( directory );
        searcher = new IndexSearcher( reader );
    }

    private void addUpdate( IndexPopulator populator, long nodeId, Object value ) throws IndexEntryConflictException
    {
        populator.add( singletonList( IndexQueryHelper.add( nodeId, index.schema(), value ) ) );
    }

    private static void updatePopulator(
            IndexPopulator populator,
            Iterable<IndexEntryUpdate<?>> updates,
            NodePropertyAccessor accessor ) throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = populator.newPopulatingUpdater( accessor ) )
        {
            for ( IndexEntryUpdate<?> update :  updates )
            {
                updater.process( update );
            }
        }
    }
}
