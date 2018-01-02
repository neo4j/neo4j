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
package org.neo4j.index.impl.lucene;

import org.apache.lucene.search.IndexSearcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReadOnlyIndexReferenceTest
{

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private IndexIdentifier identifier = mock( IndexIdentifier.class );
    private IndexSearcher searcher = mock( IndexSearcher.class );
    private CloseTrackingIndexReader reader = new CloseTrackingIndexReader();
    private ReadOnlyIndexReference indexReference = new ReadOnlyIndexReference( identifier, searcher );

    @Before
    public void setUp()
    {
        when( searcher.getIndexReader() ).thenReturn( reader );
    }

    @Test
    public void obtainingWriterIsUnsupported() throws Exception
    {
        expectedException.expect( UnsupportedOperationException.class );
        expectedException.expectMessage( "Read only indexes do not have index writers." );
        indexReference.getWriter();
    }

    @Test
    public void markAsStaleIsUnsupported()
    {
        expectedException.expect( UnsupportedOperationException.class );
        expectedException.expectMessage( "Read only indexes can't be marked as stale." );
        indexReference.setStale();
    }

    @Test
    public void checkAndClearStaleAlwaysFalse()
    {
        assertFalse( indexReference.checkAndClearStale() );
    }

    @Test
    public void disposeClosingSearcherAndMarkAsClosed() throws IOException
    {
        indexReference.dispose();

        verify( searcher ).close();
        assertTrue( reader.isClosed() );
        assertTrue( indexReference.isClosed() );
    }

    @Test
    public void detachIndexReferenceWhenSomeReferencesExist() throws IOException
    {
        indexReference.incRef();
        indexReference.detachOrClose();

        assertTrue( "Should leave index in detached state.", indexReference.isDetached() );
    }

    @Test
    public void closeIndexReferenceWhenNoReferenceExist() throws IOException
    {
        indexReference.detachOrClose();

        assertFalse( "Should leave index in closed state.", indexReference.isDetached() );
        verify( searcher ).close();
        assertTrue( reader.isClosed() );
        assertTrue( indexReference.isClosed() );
    }

    @Test
    public void doNotCloseInstanceWhenSomeReferenceExist()
    {
        indexReference.incRef();
        assertFalse( indexReference.close() );

        assertFalse( indexReference.isClosed() );
    }

    @Test
    public void closeDetachedIndexReferencedOnlyOnce() throws IOException
    {
        indexReference.incRef();
        indexReference.detachOrClose();

        assertTrue( "Should leave index in detached state.", indexReference.isDetached() );

        assertTrue( indexReference.close() );
        verify( searcher ).close();
        assertTrue( reader.isClosed() );
        assertTrue( indexReference.isClosed() );
    }

    @Test
    public void doNotCloseDetachedIndexReferencedMoreThenOnce() throws IOException
    {
        indexReference.incRef();
        indexReference.incRef();
        indexReference.detachOrClose();

        assertTrue( "Should leave index in detached state.", indexReference.isDetached() );

        assertFalse( indexReference.close() );
    }

    @Test
    public void doNotCloseReferencedIndex()
    {
        indexReference.incRef();
        assertFalse( indexReference.close() );
        assertFalse( indexReference.isClosed() );
    }

    @Test
    public void closeNotReferencedIndex()
    {
        assertTrue( indexReference.close() );
    }
}
