/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.mockito.Matchers;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.kernel.monitoring.Monitors;

public class NaiveDurableRaftLogTest
{
    @Test
    public void shouldCallWriteAllWhenStoringEntries() throws Exception
    {
        // Given
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        StoreFileChannel entriesChannel = mock( StoreFileChannel.class );
        StoreFileChannel contentChannel = mock( StoreFileChannel.class );
        StoreFileChannel commitChannel = mock( StoreFileChannel.class );

        File directory = new File(".");

        File entriesFile = new File( directory, "entries.log");
        File contentFile = new File( directory, "content.log");
        File commitFile = new File( directory, "commit.log");

        when( fsa.open( Matchers.eq( entriesFile ), anyString() ) ).thenReturn( entriesChannel );
        when( fsa.open( Matchers.eq( contentFile ), anyString() ) ).thenReturn( contentChannel );
        when( fsa.open( Matchers.eq( commitFile ), anyString() ) ).thenReturn( commitChannel );

        NaiveDurableRaftLog log = new NaiveDurableRaftLog( fsa, directory, new DummyRaftableContentSerializer(), new Monitors() );

        // When
        log.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1 ) ) );

        // Then
        verify( entriesChannel ).writeAll( any( ByteBuffer.class ), anyInt()  );
        verify( entriesChannel ).force( anyBoolean() );
        verify( contentChannel, times( 2 ) ).writeAll( any( ByteBuffer.class ), anyInt()  );
        verify( contentChannel ).force( anyBoolean() );

        // When
        log.commit( 2 );

        // Then
        verify( commitChannel ).writeAll( any( ByteBuffer.class ), anyInt()  );
        verify( commitChannel ).force( anyBoolean() );
    }
}