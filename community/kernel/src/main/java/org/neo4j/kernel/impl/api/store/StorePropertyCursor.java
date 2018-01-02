/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.Record;

/**
 * Cursor for all properties on a node or relationship.
 */
public class StorePropertyCursor implements Cursor<PropertyItem>, PropertyItem
{
    private final PropertyStore propertyStore;
    private final Consumer<StorePropertyCursor> instanceCache;
    private final StorePropertyPayloadCursor payload;

    private long nextPropertyRecordId;
    private Lock lock;

    public StorePropertyCursor( PropertyStore propertyStore, Consumer<StorePropertyCursor> instanceCache )
    {
        this.propertyStore = propertyStore;
        this.instanceCache = instanceCache;
        this.payload = new StorePropertyPayloadCursor( propertyStore.getStringStore(), propertyStore.getArrayStore() );
    }

    public StorePropertyCursor init( long firstPropertyId, Lock lock )
    {
        nextPropertyRecordId = firstPropertyId;
        this.lock = lock;
        return this;
    }

    @Override
    public boolean next()
    {
        if ( payload.next() )
        {
            return true;
        }

        while ( nextPropertyRecordId != Record.NO_NEXT_PROPERTY.intValue() )
        {
            try ( PageCursor cursor = propertyStore.newReadCursor( nextPropertyRecordId ) )
            {
                int offset = cursor.getOffset();
                do
                {
                    cursor.setOffset( offset );
                    nextPropertyRecordId = readNextPropertyRecordId( cursor );

                    payload.clear();
                    payload.init( cursor );
                }
                while ( cursor.shouldRetry() );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }

            if ( payload.next() )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public int propertyKeyId()
    {
        return payload.propertyKeyId();
    }

    @Override
    public Object value()
    {
        return payloadValueAsObject( payload );
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
        }
        finally
        {
            lock.release();
        }
    }

    static Object payloadValueAsObject( StorePropertyPayloadCursor payload )
    {
        switch ( payload.type() )
        {
        case BOOL:
            return payload.booleanValue();
        case BYTE:
            return payload.byteValue();
        case SHORT:
            return payload.shortValue();
        case CHAR:
            return payload.charValue();
        case INT:
            return payload.intValue();
        case LONG:
            return payload.longValue();
        case FLOAT:
            return payload.floatValue();
        case DOUBLE:
            return payload.doubleValue();
        case SHORT_STRING:
            return payload.shortStringValue();
        case STRING:
            return payload.stringValue();
        case SHORT_ARRAY:
            return payload.shortArrayValue();
        case ARRAY:
            return payload.arrayValue();
        default:
            throw new IllegalStateException( "No such type:" + payload.type() );
        }
    }

    private static long readNextPropertyRecordId( PageCursor cursor )
    {
        byte modifiers = cursor.getByte();
        // We don't care about previous pointer (prevProp)
        cursor.getUnsignedInt();
        long nextProp = cursor.getUnsignedInt();

        long nextMod = (modifiers & 0x0FL) << 32;
        return longFromIntAndMod( nextProp, nextMod );
    }

    private static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }
}
