/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.impl.lucene.explicit;

import org.apache.lucene.search.IndexSearcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReadOnlyIndexReferenceTest
{

    private IndexIdentifier identifier = mock( IndexIdentifier.class );
    private IndexSearcher searcher = mock( IndexSearcher.class );
    private CloseTrackingIndexReader reader = new CloseTrackingIndexReader();
    private ReadOnlyIndexReference indexReference = new ReadOnlyIndexReference( identifier, searcher );

    @BeforeEach
    void setUp()
    {
        when( searcher.getIndexReader() ).thenReturn( reader );
    }

    @Test
    void obtainingWriterIsUnsupported()
    {
        UnsupportedOperationException uoe = assertThrows( UnsupportedOperationException.class, () -> indexReference.getWriter() );
        assertEquals( uoe.getMessage(), "Read only indexes do not have index writers." );
    }

    @Test
    void markAsStaleIsUnsupported()
    {
        UnsupportedOperationException uoe = assertThrows( UnsupportedOperationException.class, () -> indexReference.setStale() );
        assertEquals( uoe.getMessage(), "Read only indexes can't be marked as stale." );
    }

    @Test
    void checkAndClearStaleAlwaysFalse()
    {
        assertFalse( indexReference.checkAndClearStale() );
    }

    @Test
    void disposeClosingSearcherAndMarkAsClosed() throws IOException
    {
        indexReference.dispose();

        assertTrue( reader.isClosed() );
        assertTrue( indexReference.isClosed() );
    }

    @Test
    void detachIndexReferenceWhenSomeReferencesExist() throws IOException
    {
        indexReference.incRef();
        indexReference.detachOrClose();

        assertTrue( indexReference.isDetached(), "Should leave index in detached state." );
    }

    @Test
    void closeIndexReferenceWhenNoReferenceExist() throws IOException
    {
        indexReference.detachOrClose();

        assertFalse( indexReference.isDetached(), "Should leave index in closed state." );
        assertTrue( reader.isClosed() );
        assertTrue( indexReference.isClosed() );
    }

    @Test
    void doNotCloseInstanceWhenSomeReferenceExist()
    {
        indexReference.incRef();
        assertFalse( indexReference.close() );

        assertFalse( indexReference.isClosed() );
    }

    @Test
    void closeDetachedIndexReferencedOnlyOnce() throws IOException
    {
        indexReference.incRef();
        indexReference.detachOrClose();

        assertTrue( indexReference.isDetached(), "Should leave index in detached state." );

        assertTrue( indexReference.close() );
        assertTrue( reader.isClosed() );
        assertTrue( indexReference.isClosed() );
    }

    @Test
    void doNotCloseDetachedIndexReferencedMoreThenOnce() throws IOException
    {
        indexReference.incRef();
        indexReference.incRef();
        indexReference.detachOrClose();

        assertTrue( indexReference.isDetached(), "Should leave index in detached state." );

        assertFalse( indexReference.close() );
    }

    @Test
    void doNotCloseReferencedIndex()
    {
        indexReference.incRef();
        assertFalse( indexReference.close() );
        assertFalse( indexReference.isClosed() );
    }

    @Test
    void closeNotReferencedIndex()
    {
        assertTrue( indexReference.close() );
    }
}
