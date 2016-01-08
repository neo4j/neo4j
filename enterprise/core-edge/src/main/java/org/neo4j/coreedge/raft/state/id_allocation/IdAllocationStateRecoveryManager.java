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
package org.neo4j.coreedge.raft.state.id_allocation;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static org.neo4j.coreedge.raft.state.id_allocation.InMemoryIdAllocationState.InMemoryIdAllocationStateMarshal.NUMBER_OF_BYTES_PER_WRITE;

public class IdAllocationStateRecoveryManager extends StateRecoveryManager
{
    public IdAllocationStateRecoveryManager( FileSystemAbstraction fileSystem)
    {
        super( fileSystem );
    }

    @Override
    protected long getLogIndex( File file ) throws IOException
    {
        long newPosition = beginningOfLastCompleteEntry( file );

        if ( newPosition < 0 )
        {
            return newPosition;
        }

        ByteBuffer buffer = ByteBuffer.allocate( (int) fileSystem.getFileSize( file ) );

        StoreChannel channel = fileSystem.open( file, "r" );

        channel.position( newPosition );

        channel.read( buffer );

        buffer.flip();

        InMemoryIdAllocationState inMemoryIdAllocationState =
                new InMemoryIdAllocationState.InMemoryIdAllocationStateMarshal().unmarshal( buffer );

        channel.close();

        return inMemoryIdAllocationState.logIndex();
    }

    /*
       * This method sets the position of the current channel to point to the beginning of the last complete entry.
       * It integer-divides the file size by the entry size (thus finding the number of complete entries), it then
       * subtracts one (which is the index of the next-to-last entry) and then multiplies by the entry size, which
       * finds the end of the next-to-last entry and therefore the beginning of the last complete entry.
       * It is assumed that the currentChannel contains at least one complete entry.
       */
    private long beginningOfLastCompleteEntry( File storeFile ) throws IOException
    {
        if ( storeFile == null )
        {
            return -1;
        }
        if ( fileSystem.getFileSize( storeFile ) < NUMBER_OF_BYTES_PER_WRITE )
        {
            return -1;
        }

        long fileSize = fileSystem.getFileSize( storeFile );
        long positionOfLastCompleteEntry =
                ((fileSize / NUMBER_OF_BYTES_PER_WRITE) - 1)
                        * NUMBER_OF_BYTES_PER_WRITE;
        return positionOfLastCompleteEntry;
    }
}
