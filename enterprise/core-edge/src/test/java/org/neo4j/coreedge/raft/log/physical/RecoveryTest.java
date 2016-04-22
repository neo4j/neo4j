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
package org.neo4j.coreedge.raft.log.physical;

import java.io.File;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.EntryReader;
import org.neo4j.coreedge.raft.log.RaftLogAppendRecord;
import org.neo4j.coreedge.raft.log.RaftLogEntry;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.kernel.impl.util.IOCursors.cursor;

public class RecoveryTest
{
    @Test
    public void shouldRecoverStateFromFiles() throws Exception
    {
        // given
        VersionFiles versionFiles = mock( VersionFiles.class );
        when( versionFiles.filesInVersionOrder() )
                .thenReturn( asList( new File( "v1 " ), new File( "v2 " ), new File( "v3 " ) ) );

        HeaderReader headerReader = mock( HeaderReader.class );
        when( headerReader.readHeader( new File( "v1 " ) ) ).thenReturn( new Header( 1, -1, -1 ) );
        when( headerReader.readHeader( new File( "v2 " ) ) ).thenReturn( new Header( 2, 9, 0 ) );
        when( headerReader.readHeader( new File( "v3 " ) ) ).thenReturn( new Header( 3, 19, 0 ) );

        EntryReader entryReader = mock( EntryReader.class );
        when( entryReader.readEntriesInVersion( 3 ) ).thenReturn( cursor(
                new RaftLogAppendRecord( 20, new RaftLogEntry( 0, valueOf( 120 ) ) ),
                new RaftLogAppendRecord( 21, new RaftLogEntry( 0, valueOf( 121 ) ) ),
                new RaftLogAppendRecord( 22, new RaftLogEntry( 0, valueOf( 122 ) ) )
        ) );

        Recovery recovery = new Recovery( versionFiles, headerReader, entryReader );

        // when
        Recovery.LogState state = recovery.recover();

        // then
        assertEquals( -1, state.prevIndex );
        assertEquals( -1, state.prevTerm );
        assertEquals( 22, state.appendIndex );
        assertEquals( 3, state.currentVersion );
        assertEquals( 1, state.ranges.lowestVersion() );
        assertEquals( 3, state.ranges.highestVersion() );
    }

    @Test
    public void shouldRecoverStateFromPrunedFiles() throws Exception
    {
        // given
        VersionFiles versionFiles = mock( VersionFiles.class );
        when( versionFiles.filesInVersionOrder() )
                .thenReturn( asList( new File( "v2 " ), new File( "v3 " ) ) );

        HeaderReader headerReader = mock( HeaderReader.class );
        when( headerReader.readHeader( new File( "v2 " ) ) ).thenReturn( new Header( 2, 9, 0 ) );
        when( headerReader.readHeader( new File( "v3 " ) ) ).thenReturn( new Header( 3, 19, 0 ) );

        EntryReader entryReader = mock( EntryReader.class );
        when( entryReader.readEntriesInVersion( 3 ) ).thenReturn( cursor() );

        Recovery recovery = new Recovery( versionFiles, headerReader, entryReader );

        // when
        Recovery.LogState state = recovery.recover();

        // then
        assertEquals( 9, state.prevIndex );
        assertEquals( 0, state.prevTerm );
        assertEquals( 19, state.appendIndex );
        assertEquals( 3, state.currentVersion );
        assertEquals( 2, state.ranges.lowestVersion() );
        assertEquals( 3, state.ranges.highestVersion() );
    }
}
