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

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TrackingLuceneIndexWriterTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldSilentlyAddDocumentsWhenMaxDocIsLessThanLimit() throws Exception
    {
        // Given
        int maxDocLimit = 1_000;
        int toAdd = maxDocLimit - 10;

        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        TrackingLuceneIndexWriter indexWriter = newTrackingLuceneIndexWriter( maxDocLimit );

        // When
        for ( int i = 0; i < toAdd; i++ )
        {
            indexWriter.addDocument( documentStructure.newDocument( i ) );
        }

        // Then
        assertEquals( toAdd, indexWriter.createSearcherManager().acquire().getIndexReader().numDocs() );
    }

    @Test
    public void shouldThrowExceptionWhenTooManyDocumentsAreAdded() throws Exception
    {
        // Given
        int maxDocLimit = 1_000;

        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        TrackingLuceneIndexWriter indexWriter = newTrackingLuceneIndexWriter( maxDocLimit );

        for ( int i = 0; i < maxDocLimit; i++ )
        {
            indexWriter.addDocument( documentStructure.newDocument( i ) );
        }

        try
        {
            // When
            indexWriter.addDocument( documentStructure.newDocument( maxDocLimit + 42 ) );
            fail( "Should have thrown " + IndexCapacityExceededException.class.getSimpleName() );
        }
        catch ( IndexCapacityExceededException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Index contains too many entries" ) );
        }
    }

    @Test
    public void shouldThrowExceptionWhenTooManyDocumentsAreUpdated() throws Exception
    {
        // Given
        int maxDocLimit = 1_000;

        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        TrackingLuceneIndexWriter indexWriter = newTrackingLuceneIndexWriter( maxDocLimit );

        for ( int i = 0; i < maxDocLimit; i++ )
        {
            indexWriter.updateDocument( new Term( "foo", "bar" ), documentStructure.newDocument( i ) );
        }

        try
        {
            // When
            indexWriter.updateDocument( new Term( "foo", "bar" ), documentStructure.newDocument( maxDocLimit + 42 ) );
            fail( "Should have thrown " + IndexCapacityExceededException.class.getSimpleName() );
        }
        catch ( IndexCapacityExceededException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Index contains too many entries" ) );
        }
    }

    private TrackingLuceneIndexWriter newTrackingLuceneIndexWriter( long maxDocLimit ) throws Exception
    {
        File luceneDir = new File( "lucene" );
        fs.get().mkdir( luceneDir );
        Directory directory = new DirectoryFactory.InMemoryDirectoryFactory().open( luceneDir );
        IndexWriterConfig config = new IndexWriterConfig( Version.LUCENE_36, null );
        TrackingLuceneIndexWriter indexWriter = new TrackingLuceneIndexWriter( directory, config );
        TrackingLuceneIndexWriter indexWriterSpy = spy( indexWriter );
        when( indexWriterSpy.maxDocLimit() ).thenReturn( maxDocLimit );
        return indexWriterSpy;
    }
}
