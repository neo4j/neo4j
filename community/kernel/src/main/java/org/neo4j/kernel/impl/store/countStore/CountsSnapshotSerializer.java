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

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_STATISTICS;

public class CountsSnapshotSerializer
{
    public static void serialize( WritableLogChannel channel, CountsSnapshot countsSnapshot )
            throws UnknownKey, IOException
    {
        channel.putLong( countsSnapshot.getTxId() );
        channel.putInt( countsSnapshot.getMap().size() );

        for ( Map.Entry<CountsKey,long[]> pair : countsSnapshot.getMap().entrySet() )
        {
            CountsKey key = pair.getKey();
            long[] value = pair.getValue();

            switch ( key.recordType() )
            {

            case ENTITY_NODE:
                assert value.length == 1;
                NodeKey nodeKey = (NodeKey) key;
                channel.put( ENTITY_NODE.code );
                channel.putInt( nodeKey.getLabelId() );
                channel.putLong( value[0] );
                break;

            case ENTITY_RELATIONSHIP:
                assert value.length == 1;
                RelationshipKey relationshipKey = (RelationshipKey) key;
                channel.put( ENTITY_RELATIONSHIP.code );
                channel.putInt( relationshipKey.getStartLabelId() );
                channel.putInt( relationshipKey.getTypeId() );
                channel.putInt( relationshipKey.getEndLabelId() );
                channel.putLong( value[0] );
                break;

            case INDEX_SAMPLE:
                assert value.length == 2;
                IndexSampleKey indexSampleKey = (IndexSampleKey) key;
                channel.put( INDEX_SAMPLE.code );
                channel.putInt( indexSampleKey.labelId() );
                channel.putInt( indexSampleKey.propertyKeyId() );
                channel.putLong( value[0] );
                channel.putLong( value[1] );
                break;

            case INDEX_STATISTICS:
                assert value.length == 2;
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
}
