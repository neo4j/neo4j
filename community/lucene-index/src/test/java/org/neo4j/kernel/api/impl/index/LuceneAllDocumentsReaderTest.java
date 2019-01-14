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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LuceneAllDocumentsReaderTest
{

    private final PartitionSearcher partitionSearcher1 = createPartitionSearcher( 1, 0, 2 );
    private final PartitionSearcher partitionSearcher2 = createPartitionSearcher( 2, 1, 2 );

    public LuceneAllDocumentsReaderTest() throws IOException
    {
    }

    @Test
    public void allDocumentsMaxCount()
    {
        LuceneAllDocumentsReader allDocumentsReader = createAllDocumentsReader();
        assertEquals( 3, allDocumentsReader.maxCount());
    }

    @Test
    public void closeCorrespondingSearcherOnClose() throws IOException
    {
        LuceneAllDocumentsReader allDocumentsReader = createAllDocumentsReader();
        allDocumentsReader.close();

        verify( partitionSearcher1 ).close();
        verify( partitionSearcher2 ).close();
    }

    @Test
    public void readAllDocuments()
    {
        LuceneAllDocumentsReader allDocumentsReader = createAllDocumentsReader();
        List<Document> documents = Iterators.asList( allDocumentsReader.iterator() );

        assertEquals( "Should have 1 document from first partition and 2 from second one.", 3, documents.size() );
        assertEquals( "1", documents.get( 0 ).getField( "value" ).stringValue() );
        assertEquals( "3", documents.get( 1 ).getField( "value" ).stringValue() );
        assertEquals( "4", documents.get( 2 ).getField( "value" ).stringValue() );
    }

    private LuceneAllDocumentsReader createAllDocumentsReader()
    {
        return new LuceneAllDocumentsReader( createPartitionReaders() );
    }

    private List<LucenePartitionAllDocumentsReader> createPartitionReaders()
    {
        LucenePartitionAllDocumentsReader reader1 = new LucenePartitionAllDocumentsReader( partitionSearcher1 );
        LucenePartitionAllDocumentsReader reader2 = new LucenePartitionAllDocumentsReader( partitionSearcher2 );
        return Arrays.asList(reader1, reader2);
    }

    private static PartitionSearcher createPartitionSearcher( int maxDoc, int partition, int maxSize )
            throws IOException
    {
        PartitionSearcher partitionSearcher = mock( PartitionSearcher.class );
        IndexSearcher indexSearcher = mock( IndexSearcher.class );
        IndexReader indexReader = mock( IndexReader.class );

        when(partitionSearcher.getIndexSearcher()).thenReturn( indexSearcher );
        when( indexSearcher.getIndexReader() ).thenReturn( indexReader );
        when( indexReader.maxDoc() ).thenReturn( maxDoc );

        when( indexSearcher.doc( 0 ) ).thenReturn( createDocument( uniqueDocValue( 1, partition, maxSize ) ) );
        when( indexSearcher.doc( 1 ) ).thenReturn( createDocument( uniqueDocValue( 2, partition, maxSize ) ) );
        when( indexSearcher.doc( 2 ) ).thenReturn( createDocument( uniqueDocValue( 3, partition, maxSize ) ) );

        return partitionSearcher;
    }

    private static String uniqueDocValue( int value, int partition, int maxSize )
    {
        return String.valueOf( value + (partition * maxSize) );
    }

    private static Document createDocument( String value )
    {
        Document document = new Document();
        document.add( new StoredField( "value", value ) );
        return document;
    }
}
