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
package org.neo4j.kernel.impl.api.store;

import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.storageengine.api.PropertyItem;

import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Cursor for all properties on a node or relationship.
 */
public class StorePropertyCursor implements Cursor<PropertyItem>, PropertyItem
{
    private final PropertyStore propertyStore;
    private final Consumer<StorePropertyCursor> instanceCache;
    private final StorePropertyPayloadCursor payload;
    private final RecordCursor<PropertyRecord> recordCursor;

    private Lock lock;

    public StorePropertyCursor( PropertyStore propertyStore, Consumer<StorePropertyCursor> instanceCache )
    {
        this.propertyStore = propertyStore;
        this.instanceCache = instanceCache;
        this.payload = new StorePropertyPayloadCursor( propertyStore.getStringStore(), propertyStore.getArrayStore() );
        this.recordCursor = propertyStore.newRecordCursor( propertyStore.newRecord() );
    }

    public StorePropertyCursor init( long firstPropertyId, Lock lock )
    {
        this.lock = lock;
        recordCursor.acquire( firstPropertyId, NORMAL );
        payload.clear();
        return this;
    }

    @Override
    public boolean next()
    {
        if ( payload.next() )
        {
            return true;
        }

        if ( !recordCursor.next() )
        {
            return false;
        }
        PropertyRecord propertyRecord = recordCursor.get();
        payload.init( propertyRecord.getBlocks(), propertyRecord.getNumberOfBlocks() );

        if ( !payload.next() )
        {
            throw new NotFoundException( "Property record with id " + propertyRecord.getId() + " not in use" );
        }
        return true;
    }

    @Override
    public int propertyKeyId()
    {
        return payload.propertyKeyId();
    }

    @Override
    public Object value()
    {
        return payload.value();
    }

    @Override
    public PropertyItem get()
    {
        return this;
    }

    @Override
    public void close()
    {
        try
        {
            payload.clear();
            instanceCache.accept( this );
            recordCursor.close();
        }
        finally
        {
            lock.release();
        }
    }
}
