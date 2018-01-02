/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static java.lang.Long.parseLong;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class LuceneSchemaIndexPopulatorTest
{
    @Test
    public void addingValuesShouldPersistThem() throws Exception
    {
        // WHEN
        index.add( 1, "First" );
        index.add( 2, "Second" );
        index.add( 3, (byte)1 );
        index.add( 4, (short)2 );
        index.add( 5, 3 );
        index.add( 6, 4L );
        index.add( 7, 5F );
        index.add( 8, 6D );

        // THEN
        assertIndexedValues(
                hit( "First", 1 ),
                hit( "Second", 2 ),
                hit( (byte)1, 3 ),
                hit( (short)2, 4 ),
                hit( 3, 5 ),
                hit( 4L, 6 ),
                hit( 5F, 7 ),
                hit( 6D, 8 ) );
    }

    @Test
    public void multipleEqualValues() throws Exception
    {
        // WHEN
        index.add( 1, "value" );
        index.add( 2, "value" );
        index.add( 3, "value" );

        // THEN
        assertIndexedValues(
                hit( "value", 1L, 2L, 3L ) );
    }

    @Test
    public void multipleEqualValuesWithUpdateThatRemovesOne() throws Exception
    {
        // WHEN
        index.add( 1, "value" );
        index.add( 2, "value" );
        index.add( 3, "value" );
        updatePopulator( index, asList( remove( 2, "value" ) ), indexStoreView );

        // THEN
        assertIndexedValues(
                hit( "value", 1L, 3L ) );
    }

    @Test
    public void changeUpdatesInterleavedWithAdds() throws Exception
    {
        // WHEN
        index.add( 1, "1" );
        index.add( 2, "2" );
        updatePopulator( index, asList( change( 1, "1", "1a" ) ), indexStoreView );
        index.add( 3, "3" );

        // THEN
        assertIndexedValues(
                no( "1" ),
                hit( "1a", 1 ),
                hit( "2", 2 ),
                hit( "3", 3 ) );
    }

    @Test
    public void addUpdatesInterleavedWithAdds() throws Exception
    {
        // WHEN
        index.add( 1, "1" );
        index.add( 2, "2" );
        updatePopulator( index,  asList( remove( 1, "1" ), add( 1, "1a" ) ), indexStoreView );
        index.add( 3, "3" );

        // THEN
        assertIndexedValues(
                hit( "1a", 1 ),
                hit( "2", 2 ),
                hit( "3", 3 ),
                no( "1" ) );
    }

    @Test
    public void removeUpdatesInterleavedWithAdds() throws Exception
    {
        // WHEN
        index.add( 1, "1" );
        index.add( 2, "2" );
        updatePopulator( index,  asList( remove( 2, "2" ) ), indexStoreView );
        index.add( 3, "3" );

        // THEN
        assertIndexedValues(
                hit( "1", 1 ),
                no( "2" ),
                hit( "3", 3 ) );
    }

    @Test
    public void multipleInterleaves() throws Exception
    {
        // WHEN
        index.add( 1, "1" );
        index.add( 2, "2" );
        updatePopulator( index,  asList( change( 1, "1", "1a" ), change( 2, "2", "2a" ) ), indexStoreView );
        index.add( 3, "3" );
        index.add( 4, "4" );
        updatePopulator( index,  asList( change( 1, "1a", "1b" ), change( 4, "4", "4a" ) ), indexStoreView );

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
        private final Object value;
        private final Long[] nodeIds;

        Hit( Object value, Long... nodeIds )
        {
            this.value = value;
            this.nodeIds = nodeIds;
        }
    }

    private NodePropertyUpdate add( long nodeId, Object value )
    {
        return NodePropertyUpdate.add( nodeId, 0, value, new long[0] );
    }

    private NodePropertyUpdate change( long nodeId, Object valueBefore, Object valueAfter )
    {
        return NodePropertyUpdate.change( nodeId, 0, valueBefore, new long[0], valueAfter, new long[0] );
    }

    private NodePropertyUpdate remove( long nodeId, Object removedValue )
    {
        return NodePropertyUpdate.remove( nodeId, 0, removedValue, new long[0] );
    }

    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private IndexDescriptor indexDescriptor;
    private IndexStoreView indexStoreView;
    private LuceneSchemaIndexProvider provider;
    private Directory directory;
    private IndexPopulator index;
    private IndexReader reader;
    private IndexSearcher searcher;
    private final long indexId = 0;
    private final int propertyKeyId = 666;
    private final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();

    @Before
    public void before() throws Exception
    {
        directory = new RAMDirectory();
        DirectoryFactory directoryFactory = new DirectoryFactory.Single(
                new DirectoryFactory.UncloseableDirectory( directory ) );
        provider = new LuceneSchemaIndexProvider( fs.get(), directoryFactory, new File( "target/whatever" ),
                NullLogProvider.getInstance(), new Config(), OperationalMode.single  );
        indexDescriptor = new IndexDescriptor( 42, propertyKeyId );
        indexStoreView = mock( IndexStoreView.class );
        IndexConfiguration indexConfig = new IndexConfiguration( false );
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( new Config() );
        index = provider.getPopulator( indexId, indexDescriptor, indexConfig, samplingConfig );
        index.create();
    }

    @After
    public void after() throws Exception
    {
        if ( reader != null )
            reader.close();
        directory.close();
    }

    private void assertIndexedValues( Hit... expectedHits ) throws IOException, IndexCapacityExceededException
    {
        switchToVerification();

        for ( Hit hit : expectedHits )
        {
            TopDocs hits = searcher.search( documentLogic.newSeekQuery( hit.value ), 10 );
            assertEquals( "Unexpected number of index results from " + hit.value, hit.nodeIds.length, hits.totalHits );
            Set<Long> foundNodeIds = new HashSet<>();
            for ( int i = 0; i < hits.totalHits; i++ )
            {
                Document document = searcher.doc( hits.scoreDocs[i].doc );
                foundNodeIds.add( parseLong( document.get( "id" ) ) );
            }
            assertEquals( asSet( hit.nodeIds ), foundNodeIds );
        }
    }

    private void switchToVerification() throws IOException, IndexCapacityExceededException
    {
        index.close( true );
        assertEquals( InternalIndexState.ONLINE, provider.getInitialState( indexId ) );
        reader = IndexReader.open( directory );
        searcher = new IndexSearcher( reader );
    }

    private static void updatePopulator(
            IndexPopulator populator,
            Iterable<NodePropertyUpdate> updates,
            PropertyAccessor accessor )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
    {
        try ( IndexUpdater updater = populator.newPopulatingUpdater( accessor ) )
        {
            for ( NodePropertyUpdate update :  updates )
            {
                updater.process( update );
            }
        }
    }
}
