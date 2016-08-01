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
package org.neo4j.coreedge.core.state.snapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.coreedge.core.state.storage.SafeChannelMarshal;
import org.neo4j.coreedge.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;

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

    public static class Marshal extends SafeChannelMarshal<CoreSnapshot>
    {
        @Override
        public void marshal( CoreSnapshot coreSnapshot, WritableChannel buffer ) throws IOException
        {
            buffer.putLong( coreSnapshot.prevIndex );
            buffer.putLong( coreSnapshot.prevTerm );

            buffer.putInt( coreSnapshot.size() );
            for ( CoreStateType type : coreSnapshot.types() )
            {
                buffer.putInt( type.ordinal() );
                type.marshal.marshal( coreSnapshot.get( type ), buffer );
            }
        }

        @Override
        public CoreSnapshot unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long prevIndex = channel.getLong();
            long prevTerm = channel.getLong();

            CoreSnapshot coreSnapshot = new CoreSnapshot( prevIndex, prevTerm );
            int snapshotCount = channel.getInt();
            for ( int i = 0; i < snapshotCount; i++ )
            {
                int typeOrdinal = channel.getInt();
                CoreStateType type = CoreStateType.values()[typeOrdinal];
                Object state = type.marshal.unmarshal( channel );
                coreSnapshot.add( type, state );
            }

            return coreSnapshot;
        }
    }

    @Override
    public String toString()
    {
        return format( "CoreSnapshot{prevIndex=%d, prevTerm=%d, snapshotCollection=%s}", prevIndex, prevTerm, snapshotCollection );
    }
}
