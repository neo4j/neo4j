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


import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
    private CloseTrackingIndexReader reader = new CloseTrackingIndexReader();
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

}
