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
package org.neo4j.index.impl.lucene;


import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WritableIndexReferenceTest
{

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private IndexIdentifier identifier = mock( IndexIdentifier.class );
    private IndexSearcher searcher = mock( IndexSearcher.class );
    private IndexWriter indexWriter = mock( IndexWriter.class );
    private WritableIndexReferenceTest.TestIndexReader reader = new WritableIndexReferenceTest.TestIndexReader();
    private WritableIndexReference indexReference = new WritableIndexReference( identifier, searcher, indexWriter);

    @Before
    public void setUp()
    {
        when( searcher.getIndexReader() ).thenReturn( reader );
    }


    @Test
    public void useProvidedWriterAsIndexWriter() throws Exception
    {
        assertSame( indexWriter, indexReference.getWriter() );
    }

    @Test
    public void stalingWritableIndex() throws Exception
    {
        assertFalse( "Index is not stale by default.", indexReference.checkAndClearStale() );
        indexReference.setStale();
        assertTrue( "We should be able to reset stale index state.", indexReference.checkAndClearStale() );
        assertFalse( "Index is not stale anymore.", indexReference.checkAndClearStale() );

    }

    @Test
    public void disposeWritableIndex() throws Exception
    {
        indexReference.dispose();
        assertTrue( "Reader should be closed.", reader.isClosed() );
        assertTrue( "Writer should be closed.", indexReference.isWriterClosed() );
    }

    private class TestIndexReader extends IndexReader
    {
        private boolean closed = false;

        @Override
        public TermFreqVector[] getTermFreqVectors( int docNumber ) throws IOException
        {
            return new TermFreqVector[0];
        }

        @Override
        public TermFreqVector getTermFreqVector( int docNumber, String field ) throws IOException
        {
            return null;
        }

        @Override
        public void getTermFreqVector( int docNumber, String field, TermVectorMapper mapper ) throws IOException
        {

        }

        @Override
        public void getTermFreqVector( int docNumber, TermVectorMapper mapper ) throws IOException
        {

        }

        @Override
        public int numDocs()
        {
            return 0;
        }

        @Override
        public int maxDoc()
        {
            return 0;
        }

        @Override
        public Document document( int n, FieldSelector fieldSelector ) throws CorruptIndexException, IOException
        {
            return null;
        }

        @Override
        public boolean isDeleted( int n )
        {
            return false;
        }

        @Override
        public boolean hasDeletions()
        {
            return false;
        }

        @Override
        public byte[] norms( String field ) throws IOException
        {
            return new byte[0];
        }

        @Override
        public void norms( String field, byte[] bytes, int offset ) throws IOException
        {

        }

        @Override
        protected void doSetNorm( int doc, String field, byte value ) throws CorruptIndexException, IOException
        {

        }

        @Override
        public TermEnum terms() throws IOException
        {
            return null;
        }

        @Override
        public TermEnum terms( Term t ) throws IOException
        {
            return null;
        }

        @Override
        public int docFreq( Term t ) throws IOException
        {
            return 0;
        }

        @Override
        public TermDocs termDocs() throws IOException
        {
            return null;
        }

        @Override
        public TermPositions termPositions() throws IOException
        {
            return null;
        }

        @Override
        protected void doDelete( int docNum ) throws CorruptIndexException, IOException
        {

        }

        @Override
        protected void doUndeleteAll() throws CorruptIndexException, IOException
        {

        }

        @Override
        protected void doCommit( Map<String,String> commitUserData ) throws IOException
        {

        }

        @Override
        protected void doClose() throws IOException
        {
            closed = true;
        }

        @Override
        public FieldInfos getFieldInfos()
        {
            return null;
        }

        public boolean isClosed()
        {
            return closed;
        }
    }
}