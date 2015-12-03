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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.membership.CoreMemberMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class DurableVoteStore extends LifecycleAdapter implements VoteStore<CoreMember>
{
    private final StoreChannel channel;
    private CoreMember votedFor;

    public DurableVoteStore( FileSystemAbstraction fileSystem, File directory )
    {
        try
        {
            channel = fileSystem.open( new File( directory, "vote.state" ), "rw" );
            votedFor = readVote();
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
    public CoreMember votedFor()
    {
        return votedFor;
    }

    @Override
    public void update( CoreMember votedFor ) throws RaftStorageException
    {
        try
        {
            if ( votedFor == null )
            {
                channel.truncate( 0 );
            }
            else
            {
                ByteBuf byteBuf = Unpooled.buffer();
                CoreMemberMarshal.serialize( votedFor, byteBuf );
                ByteBuffer buffer = byteBuf.nioBuffer();

                channel.writeAll( buffer, 0 );
            }
            channel.force( false );
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( "Failed to update votedFor", e );
        }
        this.votedFor = votedFor;
    }

    private CoreMember readVote() throws IOException
    {
        if ( channel.size() == 0 )
        {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.allocate( (int) channel.size() );
        channel.read( buffer, 0 );
        buffer.flip();

        return CoreMemberMarshal.deserialize( Unpooled.wrappedBuffer( buffer ) );
    }
}
