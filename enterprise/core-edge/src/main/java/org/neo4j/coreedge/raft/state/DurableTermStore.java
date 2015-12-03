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
package org.neo4j.coreedge.raft.state;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class DurableTermStore extends LifecycleAdapter implements TermStore
{
    public static final int TERM_BYTES = 8;

    private final StoreChannel channel;
    private long term;

    public DurableTermStore( FileSystemAbstraction fileSystem, File directory )
    {
        try
        {
            channel = fileSystem.open( new File( directory, "term.state" ), "rw" );
            term = readTerm();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        channel.force( false );
        channel.close();
    }

    @Override
    public long currentTerm()
    {
        return term;
    }

    @Override
    public void update( long newTerm ) throws RaftStorageException
    {
        if ( newTerm < term )
        {
            throw new IllegalArgumentException( "Cannot move to a lower term" );
        }

        try
        {
            ByteBuffer buffer = ByteBuffer.allocate( TERM_BYTES );
            buffer.putLong( newTerm );
            buffer.flip();

            channel.writeAll( buffer, 0 );
            channel.force( false );
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( "Failed to update term", e );
        }
        term = newTerm;
    }

    private long readTerm() throws IOException
    {
        if ( channel.size() < TERM_BYTES )
        {
            return 0;
        }
        ByteBuffer buffer = ByteBuffer.allocate( TERM_BYTES );
        channel.read( buffer, 0 );
        buffer.flip();
        return buffer.getLong();
    }
}
