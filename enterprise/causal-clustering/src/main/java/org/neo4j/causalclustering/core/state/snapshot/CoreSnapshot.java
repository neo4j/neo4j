/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.snapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
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
