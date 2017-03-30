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
package org.neo4j.kernel.api.impl.labelscan;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.neo4j.kernel.api.impl.index.IndexReaderStub;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartition;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.kernel.api.impl.labelscan.writer.PartitionedLuceneLabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Long.parseLong;
import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat.RANGE;

@RunWith( Parameterized.class )
public class LuceneLabelScanStoreWriterTest
{
    @Parameter
    public BitmapDocumentFormat format;

    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();
    private final DirectoryFactory dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();

    @Parameterized.Parameters( name = "{0} bits" )
    public static List<Object[]> formats()
    {
        return Stream.of( BitmapDocumentFormat.values() )
                .map( format -> new Object[]{format} )
                .collect( toList() );
    }

    @After
    public void closeDirFactory() throws Exception
    {
        System.setProperty( "labelScanStore.maxPartitionSize", "");
        dirFactory.close();
    }

    @Test
    public void shouldComplainIfNodesSuppliedOutOfRangeOrder() throws Exception
    {
        // given
        int nodeId1 = 1;
        int nodeId2 = 65;
        StubIndexPartition partition = newStubIndexPartition();

        WritableDatabaseLabelScanIndex index = prepareIndex( singletonList( partition ) );

        long node1Range = format.bitmapFormat().rangeOf( nodeId1 );
        long node2Range = format.bitmapFormat().rangeOf( nodeId2 );
        assertNotEquals( node1Range, node2Range );

        // when
        PartitionedLuceneLabelScanWriter writer = createWriter( index );
        writer.write( NodeLabelUpdate.labelChanges( nodeId2, new long[]{}, new long[]{} ) );
        try
        {
            writer.write( NodeLabelUpdate.labelChanges( nodeId1, new long[]{}, new long[]{} ) );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }

    @Test
    public void shouldStoreDocumentWithNodeIdsAndLabelsInIt() throws Exception
    {
        // given
        int nodeId = 1;
        int label1 = 201;
        int label2 = 202;
        StubIndexPartition partition = newStubIndexPartition();

        WritableDatabaseLabelScanIndex index = buildLabelScanIndex( partition );

        // when
        PartitionedLuceneLabelScanWriter writer = createWriter( index );

        writer.write( NodeLabelUpdate.labelChanges( nodeId, new long[]{}, new long[]{label1, label2} ) );
        writer.close();

        // then
        long range = format.bitmapFormat().rangeOf( nodeId );
        Document document = partition.documentFor( new Term( RANGE, valueOf( range ) ) );

        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label1 ) ) ),
                nodeId ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label2 ) ) ), nodeId ) );
    }

    @Test
    public void shouldStoreDocumentWithNodeIdsInTheSameRange() throws Exception
    {
        // given
        int nodeId1 = 1;
        int nodeId2 = 3;
        int label1 = 201;
        int label2 = 202;
        StubIndexPartition partition = newStubIndexPartition();
        long range = format.bitmapFormat().rangeOf( nodeId1 );
        assertEquals( range, format.bitmapFormat().rangeOf( nodeId2 ) );

        WritableDatabaseLabelScanIndex index = buildLabelScanIndex( partition );

        // when
        PartitionedLuceneLabelScanWriter writer = createWriter( index );

        writer.write( NodeLabelUpdate.labelChanges( nodeId1, new long[]{}, new long[]{label1} ) );
        writer.write( NodeLabelUpdate.labelChanges( nodeId2, new long[]{}, new long[]{label2} ) );
        writer.close();

        // then
        Document document = partition.documentFor( new Term( RANGE, valueOf( range ) ) );

        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label1 ) ) ), nodeId1 ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label2 ) ) ), nodeId2 ) );
    }

    @Test
    public void shouldStoreDocumentWithNodeIdsInADifferentRange() throws Exception
    {
        // given
        int nodeId1 = 1;
        int nodeId2 = 65;
        int label1 = 201;
        int label2 = 202;
        StubIndexPartition partition = newStubIndexPartition();

        long node1Range = format.bitmapFormat().rangeOf( nodeId1 );
        long node2Range = format.bitmapFormat().rangeOf( nodeId2 );
        assertNotEquals( node1Range, node2Range );

        WritableDatabaseLabelScanIndex index = buildLabelScanIndex( partition );

        // when
        PartitionedLuceneLabelScanWriter writer = createWriter( index );

        writer.write( NodeLabelUpdate.labelChanges( nodeId1, new long[]{}, new long[]{label1} ) );
        writer.write( NodeLabelUpdate.labelChanges( nodeId2, new long[]{}, new long[]{label2} ) );
        writer.close();

        // then
        Document document1 = partition.documentFor( new Term( RANGE, valueOf( node1Range ) ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document1.get( valueOf( label1 ) ) ), nodeId1 ) );

        Document document2 = partition.documentFor( new Term( RANGE, valueOf( node2Range ) ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document2.get( valueOf( label2 ) ) ), nodeId2 ) );
    }

    @Test
    public void shouldUpdateExistingDocumentWithNodesInTheSameRange() throws Exception
    {
        // given
        int nodeId1 = 1;
        int nodeId2 = 3;
        int label1 = 201;
        int label2 = 202;
        StubIndexPartition partition = newStubIndexPartition();

        long range = format.bitmapFormat().rangeOf( nodeId1 );
        assertEquals( range, format.bitmapFormat().rangeOf( nodeId2 ) );

        WritableDatabaseLabelScanIndex index = buildLabelScanIndex( partition );

        // node already indexed
        PartitionedLuceneLabelScanWriter writer = createWriter( index );
        writer.write( NodeLabelUpdate.labelChanges( nodeId1, new long[]{}, new long[]{label1} ) );
        writer.close();

        // when
        writer = createWriter( index );
        writer.write( NodeLabelUpdate.labelChanges( nodeId2, new long[]{}, new long[]{label2} ) );
        writer.close();

        // then
        Document document = partition.documentFor( new Term( RANGE, valueOf( range ) ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label1 ) ) ), nodeId1 ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label2 ) ) ), nodeId2 ) );
    }

    @Test
    public void automaticPartitionCreation() throws IOException
    {
        int nodeForPartition1 = 1;
        int nodeForPartition2 = (format.bitmapFormat().rangeSize() * 2) + 1;
        int labelNode1 = 201;
        int labelNode2 = 202;

        System.setProperty( "labelScanStore.maxPartitionSize", "2" );

        StubIndexPartition partition = newStubIndexPartition();
        WritableDatabaseLabelScanIndex index = buildLabelScanIndex( partition );
        PartitionedLuceneLabelScanWriter writer = createWriter( index );

        writer.write( NodeLabelUpdate.labelChanges( nodeForPartition1, new long[]{}, new long[]{labelNode1} ) );

        writer.write( NodeLabelUpdate.labelChanges( nodeForPartition2, new long[]{}, new long[]{labelNode2} ) );
        writer.close();
        assertEquals("We should have 2 index partitions", 2, index.getPartitions().size());
    }

    private PartitionedLuceneLabelScanWriter createWriter( WritableDatabaseLabelScanIndex index )
    {
        return new PartitionedLuceneLabelScanWriter( index, format );
    }

    private StubIndexPartition newStubIndexPartition( File folder )
    {
        try
        {
            Directory directory = dirFactory.open( folder );
            return new StubIndexPartition( folder, directory );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private StubIndexPartition newStubIndexPartition()
    {
        File folder = testDir.directory();
        return newStubIndexPartition( folder );
    }

    private static PartitionSearcher newStubPartitionSearcher( Map<Term,Document> storage )
    {
        PartitionSearcher partitionSearcher = mock( PartitionSearcher.class );
        when( partitionSearcher.getIndexSearcher() ).thenReturn( new StubIndexSearcher( storage ) );
        return partitionSearcher;
    }

    @SuppressWarnings( "unchecked" )
    private static IndexWriter newStubIndexWriter( Map<Term,Document> storage )
    {
        IndexWriter writer = mock( IndexWriter.class );

        try
        {
            doAnswer( invocation ->
            {
                Object[] args = invocation.getArguments();
                Term term = (Term) args[0];
                Iterable<? extends IndexableField> fields = (Iterable<? extends IndexableField>) args[1];
                Document document = new Document();
                fields.forEach( document::add );
                storage.put( term, document );
                return null;
            } ).when( writer ).updateDocument( any(), any() );

            doAnswer( invocation ->
            {
                Term[] terms = (Term[]) invocation.getArguments()[0];
                Stream.of( terms ).forEach( storage::remove );
                return null;
            } ).when( writer ).deleteDocuments( Mockito.<Term>anyVararg() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        return writer;
    }

    private static class StubIndexPartition extends WritableIndexPartition
    {
        final Directory directory;
        final Map<Term,Document> storage = new HashMap<>();

        StubIndexPartition( File folder, Directory directory ) throws IOException
        {
            super( folder, directory, IndexWriterConfigs.standard() );
            this.directory = directory;
        }

        @Override
        public PartitionSearcher acquireSearcher() throws IOException
        {
            return newStubPartitionSearcher( storage );
        }

        @Override
        public IndexWriter getIndexWriter()
        {
            return newStubIndexWriter( storage );
        }

        Document documentFor( Term term )
        {
            return storage.get( term );
        }
    }

    private static class StubIndexSearcher extends IndexSearcher
    {
        final Map<Term,Document> storage;
        final Map<Integer,Document> docIds;

        StubIndexSearcher( Map<Term,Document> storage )
        {
            super( new IndexReaderStub( false ) );
            this.storage = storage;
            this.docIds = new HashMap<>();
        }

        @Override
        public void search( Query query, Collector results ) throws IOException
        {
            Document document = storage.get( ((TermQuery) query).getTerm() );
            if ( document == null )
            {
                return;
            }

            int docId = ThreadLocalRandom.current().nextInt();
            docIds.put( docId, document );
            try
            {
                LeafCollector leafCollector = results.getLeafCollector( leafContexts.get( 0 ) );
                Scorer scorer = new ConstantScoreScorer( null, 10f, new OneDocIdIterator( docId ) );
                leafCollector.setScorer( scorer );
                leafCollector.collect( docId );
            }
            catch ( CollectionTerminatedException ignored )
            {
            }
        }

        @Override
        public TopDocs search( Query query, int n )
        {
            Document document = storage.get( ((TermQuery) query).getTerm() );
            if ( document == null )
            {
                return new TopDocs( 0, new ScoreDoc[0], 0 );
            }

            int docId = ThreadLocalRandom.current().nextInt();
            docIds.put( docId, document );
            int score = 10;
            return new TopDocs( 1, new ScoreDoc[]{new ScoreDoc( docId, score )}, score );
        }

        @Override
        public Document doc( int docID )
        {
            return docIds.get( docID );
        }
    }

    private static class OneDocIdIterator extends DocIdSetIterator
    {
        final int target;
        int currentDoc = -1;
        boolean exhausted;

        OneDocIdIterator( int docId )
        {
            target = docId;
        }

        @Override
        public int docID()
        {
            return currentDoc;
        }

        @Override
        public int nextDoc() throws IOException
        {
            return advance( currentDoc + 1 );
        }

        @Override
        public int advance( int target ) throws IOException
        {
            if ( exhausted || target > this.target )
            {
                return currentDoc = DocIdSetIterator.NO_MORE_DOCS;
            }
            else
            {
                exhausted = true;
                return currentDoc = this.target;
            }
        }

        @Override
        public long cost()
        {
            return 1L;
        }
    }

    private WritableDatabaseLabelScanIndex buildLabelScanIndex( StubIndexPartition partition ) throws IOException
    {
        List<AbstractIndexPartition> partitions = new ArrayList<>();
        partitions.add( partition );
        WritableDatabaseLabelScanIndex index = prepareIndex( partitions );

        when( index.addNewPartition() ).then( invocation ->
        {
            StubIndexPartition newPartition =
                    newStubIndexPartition( testDir.directory( String.valueOf( partitions.size() ) ) );
            partitions.add( newPartition );
            return newPartition;
        } );
        return index;
    }

    private WritableDatabaseLabelScanIndex prepareIndex( List<AbstractIndexPartition> partitions )
    {
        WritableDatabaseLabelScanIndex index = mock( WritableDatabaseLabelScanIndex.class );
        when( index.getPartitions() ).thenReturn( partitions );
        return index;
    }
}
