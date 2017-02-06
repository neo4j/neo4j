/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.countStore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.value;

public class CountsSnapshotDeserializer
{
    public static CountsSnapshot deserialize( ReadableClosableChannel channel ) throws IOException
    {
        long txid = channel.getLong();
        int size = channel.getInt();

        Map<CountsKey,long[]> map = new ConcurrentHashMap<>( size );
        CountsKey key;
        long[] value;
        for ( int i = 0; i < size; i++ )
        {
            CountsKeyType type = value( channel.get() );
            switch ( type )
            {
            case ENTITY_NODE:
                key = nodeKey( channel.getInt() );
                value = new long[]{channel.getLong()};
                map.put( key, value );
                break;

            case ENTITY_RELATIONSHIP:
                int startLabelId = channel.getInt();
                int typeId = channel.getInt();
                int endLabelId = channel.getInt();
                key = relationshipKey( startLabelId, typeId, endLabelId );
                value = new long[]{channel.getLong()};
                map.put( key, value );
                break;

            case INDEX_SAMPLE:
                key = indexSampleKey( channel.getLong() );
                value = new long[]{channel.getLong(), channel.getLong()};
                map.put( key, value );
                break;

            case INDEX_STATISTICS:
                key = indexStatisticsKey( channel.getLong() );
                value = new long[]{channel.getLong(), channel.getLong()};
                map.put( key, value );
                break;

            case EMPTY:
                throw new IllegalArgumentException( "CountsKey of type EMPTY cannot be deserialized." );

            default:
                throw new IllegalArgumentException( "The read CountsKey has an unknown type." );
            }
        }
        return new CountsSnapshot( txid, map );
    }
}
