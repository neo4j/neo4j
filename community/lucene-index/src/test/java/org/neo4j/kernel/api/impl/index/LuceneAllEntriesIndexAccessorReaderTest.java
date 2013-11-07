/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.helpers.collection.IteratorUtil;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneAllEntriesIndexAccessorReaderTest
{
    @Test
    public void shouldIterateOverAllLuceneDocumentsWhereNoDocumentsHaveBeenDeleted() throws Exception
    {
        // given
        Long[] nodeIds = {12L, 34L, 567L};

        IndexReader indexReader = mock( IndexReader.class );
        when( indexReader.isDeleted( anyInt() ) ).thenReturn( false );

        LuceneAllEntriesIndexAccessorReader reader =
                new LuceneAllEntriesIndexAccessorReader(
                        new SearcherManagerStub( new SearcherStub( indexReader, nodeIds ) ),
                        new LuceneDocumentStructure() );

        // when
        Iterator<Long> iterator = reader.iterator();
        Collection<Long> actualNodeIds = IteratorUtil.asCollection( iterator );

        // then
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            assertThat( actualNodeIds, hasItem( nodeIds[i] ) );
        }
    }

    @Test
    public void shouldFindNoDocumentsIfTheyHaveAllBeenDeleted() throws Exception
    {
        // given
        final Long[] nodeIds = {12L, 34L, 567L};

        final IndexReader indexReader = mock( IndexReader.class );
        when( indexReader.isDeleted( anyInt() ) ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                if ( (int) invocation.getArguments()[0] >= nodeIds.length )
                {
                    throw new IllegalArgumentException( "Doc id out of range" );
                }
                return true;
            }
        } );

        LuceneAllEntriesIndexAccessorReader reader =
                new LuceneAllEntriesIndexAccessorReader(
                        new SearcherManagerStub( new SearcherStub( indexReader, nodeIds ) ),
                        new LuceneDocumentStructure() );

        // when
        Iterator<Long> iterator = reader.iterator();
        Collection<Long> actualNodeIds = IteratorUtil.asCollection( iterator );

        // then
        assertTrue( actualNodeIds.isEmpty() );
    }

    private static class SearcherStub extends IndexSearcher
    {
        private final Long[] nodeIds;

        public SearcherStub( IndexReader r, Long[] nodeIds )
        {
            super( r );
            this.nodeIds = nodeIds;
        }

        @Override
        public Document doc( int docID ) throws IOException
        {
            return new LuceneDocumentStructure().newDocument( nodeIds[docID] );
        }

        @Override
        public int maxDoc()
        {
            return nodeIds.length;
        }
    }

    private static class SearcherManagerStub extends ReferenceManager<IndexSearcher>
    {
        SearcherManagerStub( IndexSearcher searcher )
        {
            this.current = searcher;
        }

        @Override
        protected void decRef( IndexSearcher reference ) throws IOException
        {
        }

        @Override
        protected IndexSearcher refreshIfNeeded( IndexSearcher referenceToRefresh ) throws IOException
        {
            return null;
        }

        @Override
        protected boolean tryIncRef( IndexSearcher reference )
        {
            return true;
        }
    }
}
