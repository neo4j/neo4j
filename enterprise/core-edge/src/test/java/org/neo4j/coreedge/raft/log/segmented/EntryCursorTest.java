/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log.segmented;

import org.junit.Test;

import java.io.File;
import java.util.Collections;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

public class EntryCursorTest
{

    @Test
    public void ifFileExistsButEntryDoesNotExist() throws Exception
    {
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        File bam = new File( "bam" );
        fsa.mkdir( bam );
        FileNames fileNames = new FileNames( bam );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );

        Segments segments = new Segments( fsa, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );

        // When
        segments.rotate( -1, -1, -1 );
        segments.rotate( 10, 10, 10 );
        segments.last().closeWriter();

        EntryCursor entryCursor = new EntryCursor( segments, 1L );

        boolean next = entryCursor.next();

        assertFalse( next );
    }

    @Test
    public void requestedSegmentHasBeenPruned() throws Exception
    {
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        File bam = new File( "bam" );
        fsa.mkdir( bam );
        FileNames fileNames = new FileNames( bam );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );

        Segments segments = new Segments( fsa, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );

        // When
        segments.rotate( -1, -1, -1 );
        segments.rotate( 10, 10, 10 );
        segments.rotate( 20, 20, 20 );
        segments.prune( 12 );
        segments.last().closeWriter();

        EntryCursor entryCursor = new EntryCursor( segments, 1L );

        boolean next = entryCursor.next();

        assertFalse( next );
    }

    @Test
    public void requestedSegmentHasNotExistedYet() throws Exception
    {
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        File bam = new File( "bam" );
        fsa.mkdir( bam );
        FileNames fileNames = new FileNames( bam );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );

        Segments segments = new Segments( fsa, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );

        // When
        segments.rotate( -1, -1, -1 );
        segments.rotate( 10, 10, 10 );
        segments.rotate( 20, 20, 20 );
        segments.last().closeWriter();

        EntryCursor entryCursor = new EntryCursor( segments, 100L );

        boolean next = entryCursor.next();

        assertFalse( next );
    }
}
