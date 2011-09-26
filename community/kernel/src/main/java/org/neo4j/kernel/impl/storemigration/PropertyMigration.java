/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class PropertyMigration
{
    private LegacyPropertyStoreReader propertyStoreReader;
    private LegacyDynamicRecordFetcher legacyDynamicRecordFetcher;
    private LegacyNodeStoreReader legacyNodeStoreReader;

    public PropertyMigration( LegacyNodeStoreReader legacyNodeStoreReader, LegacyPropertyStoreReader propertyStoreReader, LegacyDynamicRecordFetcher legacyDynamicRecordFetcher )
    {
        this.propertyStoreReader = propertyStoreReader;
        this.legacyDynamicRecordFetcher = legacyDynamicRecordFetcher;
        this.legacyNodeStoreReader = legacyNodeStoreReader;
    }

    public void migrateNodeProperties( NodeStore nodeStore, PropertyWriter propertyWriter ) throws IOException
    {
        Iterable<NodeRecord> records = legacyNodeStoreReader.readNodeStore();
        for ( NodeRecord nodeRecord : records )
        {
            if ( nodeRecord.getNextProp() != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                LegacyPropertyRecord propertyRecord = propertyStoreReader.readPropertyRecord( nodeRecord.getNextProp() );
                List<Pair<Integer, Object>> properties = new ArrayList<Pair<Integer, Object>>();
                while ( propertyRecord.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
                {
                    properties.add( extractValue( propertyRecord ) );
                    propertyRecord = propertyStoreReader.readPropertyRecord( propertyRecord.getNextProp() );
                }
                properties.add( extractValue( propertyRecord ) );
                long propertyRecordId = propertyWriter.writeProperties( properties );
                nodeRecord.setNextProp( propertyRecordId );
                nodeStore.setHighId( nodeRecord.getId() );
                nodeStore.updateRecord( nodeRecord );
            }
        }
    }

    private Pair<Integer, Object> extractValue( LegacyPropertyRecord propertyRecord )
    {
        int keyIndexId = propertyRecord.getKeyIndexId();
        Object value = propertyRecord.getType().getValue( propertyRecord, legacyDynamicRecordFetcher );
        return Pair.of( keyIndexId, value );
    }
}
