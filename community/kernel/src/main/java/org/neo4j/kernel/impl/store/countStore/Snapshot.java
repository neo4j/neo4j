/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_STATISTICS;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.value;

public class Snapshot
{
    private ConcurrentHashMap<CountsKey,long[]> map;
    private long txId;

    public Snapshot( long txId, ConcurrentHashMap<CountsKey,long[]> map )
    {
        this.map = map;
        this.txId = txId;
    }

    public ConcurrentHashMap<CountsKey,long[]> getMap()
    {
        return map;
    }

    public long getTxId()
    {
        return txId;
    }

    public static void serialize( WritableLogChannel channel, Snapshot snapshot ) throws UnknownKey, IOException
    {
        channel.putLong( snapshot.getTxId() );
        channel.putInt( snapshot.getMap().size() );

        for ( Map.Entry<CountsKey,long[]> pair : snapshot.getMap().entrySet() )
        {
            CountsKey key = pair.getKey();
            long[] value = pair.getValue();

            switch ( key.recordType() )
            {

            case ENTITY_NODE:
                NodeKey nodeKey = (NodeKey) key;
                channel.put( ENTITY_NODE.code );
                channel.putInt( nodeKey.getLabelId() );
                channel.putLong( value[0] );
                break;

            case ENTITY_RELATIONSHIP:
                RelationshipKey relationshipKey = (RelationshipKey) key;
                channel.put( ENTITY_RELATIONSHIP.code );
                channel.putInt( relationshipKey.getStartLabelId() );
                channel.putInt( relationshipKey.getTypeId() );
                channel.putInt( relationshipKey.getEndLabelId() );
                channel.putLong( value[0] );
                break;

            case INDEX_SAMPLE:
                IndexSampleKey indexSampleKey = (IndexSampleKey) key;
                channel.put( INDEX_SAMPLE.code );
                channel.putInt( indexSampleKey.labelId() );
                channel.putInt( indexSampleKey.propertyKeyId() );
                channel.putLong( value[0] );
                channel.putLong( value[1] );
                break;

            case INDEX_STATISTICS:
                IndexStatisticsKey indexStatisticsKey = (IndexStatisticsKey) key;
                channel.put( INDEX_STATISTICS.code );
                channel.putInt( indexStatisticsKey.labelId() );
                channel.putInt( indexStatisticsKey.propertyKeyId() );
                channel.putLong( value[0] );
                channel.putLong( value[1] );
                break;

            }
        }
    }

    public static Snapshot deserialize( ReadableLogChannel channel ) throws UnknownKey, IOException
    {
        long txid = channel.getLong();
        int size = channel.getInt();

        ConcurrentHashMap<CountsKey,long[]> map = new ConcurrentHashMap<>( size );
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
                key = indexSampleKey( channel.getInt(), channel.getInt() );
                value = new long[]{channel.getLong(), channel.getLong()};
                map.put( key, value );
                break;

            case INDEX_STATISTICS:
                key = indexStatisticsKey( channel.getInt(), channel.getInt() );
                value = new long[]{channel.getLong(), channel.getLong()};
                map.put( key, value );
                break;
            }
        }
        return new Snapshot( txid, map );
    }

}