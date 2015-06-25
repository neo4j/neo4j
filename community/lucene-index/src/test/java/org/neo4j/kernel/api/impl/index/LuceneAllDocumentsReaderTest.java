/*
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
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.IteratorUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LuceneAllDocumentsReaderTest
{
    @Test
    public void shouldIterateOverAllLuceneDocumentsWhereNoDocumentsHaveBeenDeleted() throws Exception
    {
        // given
        String[] elements = {"A", "B", "C"};

        LuceneAllDocumentsReader reader =
                new LuceneAllDocumentsReader(
                        new SearcherManagerStub( new SearcherStub( new IndexReaderStub( false, elements ), elements )
                        ) );

        // when
        Iterator<Document> iterator = reader.iterator();
        List<Document> actualDocuments = new ArrayList<>( IteratorUtil.asCollection( iterator ) );

        // then
        for ( int i = 0; i < elements.length; i++ )
        {
            assertEquals( elements[i], actualDocuments.get( i ).get( "element" ) );
        }
    }

    @Test
    public void shouldFindNoDocumentsIfTheyHaveAllBeenDeleted() throws Exception
    {
        // given
        final String[] elements = {"A", "B", "C"};

        LuceneAllDocumentsReader reader =
                new LuceneAllDocumentsReader(
                        new SearcherManagerStub( new SearcherStub( new IndexReaderStub( true, elements ), elements )
                        ) );

        // when
        Iterator<Document> iterator = reader.iterator();

        // then
        assertFalse( iterator.hasNext() );
    }

    private static class SearcherStub extends IndexSearcher
    {
        private final String[] elements;

        public SearcherStub( IndexReader r, String[] elements )
        {
            super( r );
            this.elements = elements;
        }

        @Override
        public Document doc( int docID ) throws IOException
        {
            Document document = new Document();
            document.add( new StoredField( "element", elements[docID] ) );
            return document;
        }
    }
}
