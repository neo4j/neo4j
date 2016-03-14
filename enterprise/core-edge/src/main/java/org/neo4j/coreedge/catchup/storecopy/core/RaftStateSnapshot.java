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
package org.neo4j.coreedge.catchup.storecopy.core;

import java.io.IOException;

import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class RaftStateSnapshot
{
    private final RaftStateType type;
    private final Object state;

    public RaftStateSnapshot( RaftStateType type, Object state )
    {
        this.type = type;
        this.state = state;
    }

    public RaftStateType type()
    {
        return type;
    }

    public Object state()
    {
        return state;
    }

    public static class Marshal implements ChannelMarshal<RaftStateSnapshot>
    {
        @Override
        public void marshal( RaftStateSnapshot snapshot, WritableChannel channel ) throws IOException
        {
            channel.putInt( snapshot.type.ordinal() );
            snapshot.type.marshal.marshal( snapshot.state, channel );
        }

        @Override
        public RaftStateSnapshot unmarshal( ReadableChannel channel ) throws IOException
        {
            int ordinal = channel.getInt();
            RaftStateType type = RaftStateType.values()[ordinal];
            Object state = type.marshal.unmarshal( channel );
            return new RaftStateSnapshot( type, state );
        }
    }
}
