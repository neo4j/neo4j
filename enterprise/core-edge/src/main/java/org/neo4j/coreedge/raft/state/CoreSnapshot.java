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
package org.neo4j.coreedge.raft.state;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.coreedge.catchup.storecopy.core.NetworkFlushableByteBuf;
import org.neo4j.coreedge.catchup.storecopy.core.CoreStateType;
import org.neo4j.coreedge.catchup.tx.edge.NetworkReadableClosableByteBuf;
import org.neo4j.coreedge.server.ByteBufMarshal;

public class CoreSnapshot
{
    private final long prevIndex;
    private final long prevTerm;

    private final Map<CoreStateType,Object> snapshotCollection = new HashMap<>();

    public CoreSnapshot( long prevIndex, long prevTerm )
    {
        this.prevIndex = prevIndex;
        this.prevTerm = prevTerm;
    }

    public long prevIndex()
    {
        return prevIndex;
    }

    public long prevTerm()
    {
        return prevTerm;
    }

    public void add( CoreStateType type, Object state )
    {
        snapshotCollection.put( type, state );
    }

    public <T> T get( CoreStateType type )
    {
        return (T) snapshotCollection.get( type );
    }

    public Iterable<CoreStateType> types()
    {
        return snapshotCollection.keySet();
    }

    public int size()
    {
        return snapshotCollection.size();
    }

    public static class Marshal implements ByteBufMarshal<CoreSnapshot>
    {
        @Override
        public void marshal( CoreSnapshot coreSnapshot, ByteBuf buffer )
        {
            buffer.writeLong( coreSnapshot.prevIndex );
            buffer.writeLong( coreSnapshot.prevTerm );

            buffer.writeInt( coreSnapshot.size() );
            for ( CoreStateType type : coreSnapshot.types() )
            {
                try
                {
                    buffer.writeInt( type.ordinal() );
                    type.marshal.marshal( coreSnapshot.get( type ), new NetworkFlushableByteBuf( buffer ) );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "Not possible" );
                }
            }
        }

        @Override
        public CoreSnapshot unmarshal( ByteBuf source )
        {
            long prevIndex = source.readLong();
            long prevTerm = source.readLong();

            CoreSnapshot coreSnapshot = new CoreSnapshot( prevIndex, prevTerm );
            int snapshotCount = source.readInt();
            for ( int i = 0; i < snapshotCount; i++ )
            {
                int typeOrdinal = source.readInt();
                try
                {
                    CoreStateType type = CoreStateType.values()[typeOrdinal];
                    Object state = type.marshal.unmarshal( new NetworkReadableClosableByteBuf( source ) );
                    coreSnapshot.add( type, state );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "Not possible" );
                }
            }

            return coreSnapshot;
        }
    }
}
