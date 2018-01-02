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

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class LuceneSnapshotterTest
{

    private final File indexDir = new File(".");
    private SnapshotDeletionPolicy snapshotPolicy;

    private IndexCommit luceneSnapshot;
    private LuceneIndexWriter writer;

    @Before
    public void setup() throws IOException
    {
        writer = mock( LuceneIndexWriter.class );
        snapshotPolicy = mock(SnapshotDeletionPolicy.class);
        luceneSnapshot = mock(IndexCommit.class);

        IndexWriterConfig config = new IndexWriterConfig( Version.LUCENE_36, null );

        when( writer.getIndexDeletionPolicy() ).thenReturn( snapshotPolicy );

        when(snapshotPolicy.snapshot( anyString() )).thenReturn( luceneSnapshot );
    }

    @Test
    public void shouldReturnRealSnapshotIfIndexAllowsIt() throws Exception
    {
        // Given
        LuceneSnapshotter snapshotter = new LuceneSnapshotter();

        when(luceneSnapshot.getFileNames()).thenReturn( asList("a", "b") );

        // When
        ResourceIterator<File> snapshot = snapshotter.snapshot( indexDir, writer );

        // Then
        assertEquals( new File(indexDir, "a"), snapshot.next() );
        assertEquals( new File(indexDir, "b"), snapshot.next() );
        assertFalse( snapshot.hasNext() );
        snapshot.close();

        verify( snapshotPolicy ).release( anyString() );
    }

    @Test
    public void shouldReturnEmptyIteratorWhenNoCommitsHaveBeenMade() throws Exception
    {
        // Given
        LuceneSnapshotter snapshotter = new LuceneSnapshotter();

        when(luceneSnapshot.getFileNames()).thenThrow( new IllegalStateException( "No index commit to snapshot" ));

        // When
        ResourceIterator<File> snapshot = snapshotter.snapshot( indexDir, writer );

        // Then
        assertFalse( snapshot.hasNext() );
        snapshot.close();

        verify( snapshotPolicy ).snapshot( anyString() );
        verifyNoMoreInteractions( snapshotPolicy );
    }


}
