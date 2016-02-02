/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.storageengine.api.ReadableChannel;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.value;

public class CountsSnapshotDeserializer
{
    public static CountsSnapshot deserialize( ReadableChannel data ) throws IOException
    {
        long txid = data.getLong();
        int size = data.getInt();

        Map<CountsKey,long[]> map = new ConcurrentHashMap<>( size );
        CountsKey key;
        long[] value;
        for ( int i = 0; i < size; i++ )
        {
            CountsKeyType type = value( data.get() );
            switch ( type )
            {
            case ENTITY_NODE:
                key = nodeKey( data.getInt() );
                value = new long[]{data.getLong()};
                map.put( key, value );
                break;

            case ENTITY_RELATIONSHIP:
                int startLabelId = data.getInt();
                int typeId = data.getInt();
                int endLabelId = data.getInt();
                key = relationshipKey( startLabelId, typeId, endLabelId );
                value = new long[]{data.getLong()};
                map.put( key, value );
                break;

            case INDEX_SAMPLE:
                key = indexSampleKey( data.getInt(), data.getInt() );
                value = new long[]{data.getLong(), data.getLong()};
                map.put( key, value );
                break;

            case INDEX_STATISTICS:
                key = indexStatisticsKey( data.getInt(), data.getInt() );
                value = new long[]{data.getLong(), data.getLong()};
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
