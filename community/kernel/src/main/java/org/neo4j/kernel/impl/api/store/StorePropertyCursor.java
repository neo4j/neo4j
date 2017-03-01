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
package org.neo4j.kernel.impl.api.store;

import java.util.function.Consumer;
import java.util.function.IntPredicate;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.storageengine.api.PropertyItem;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Cursor for all properties on a node or relationship.
 */
public class StorePropertyCursor implements Cursor<PropertyItem>, PropertyItem
{
    private final Consumer<StorePropertyCursor> instanceCache;
    private final StorePropertyPayloadCursor payload;
    private final RecordCursor<PropertyRecord> recordCursor;

    private boolean fetched;
    private Lock lock;
    private IntPredicate propertyKeyIds;

    public StorePropertyCursor( RecordCursors cursors, Consumer<StorePropertyCursor> instanceCache )
    {
        this.instanceCache = instanceCache;
        this.payload = new StorePropertyPayloadCursor( cursors.propertyString(), cursors.propertyArray() );
        this.recordCursor = cursors.property();
    }

    public StorePropertyCursor init( long firstPropertyId, Lock lock )
    {
        return init( ALWAYS_TRUE_INT, firstPropertyId, lock );
    }

    public StorePropertyCursor init( int propertyKeyId, long firstPropertyId, Lock lock )
    {
        return init( key -> key == propertyKeyId, firstPropertyId, lock );
    }

    public StorePropertyCursor init( IntPredicate propertyKeyIds, long firstPropertyId, Lock lock )
    {
        this.propertyKeyIds = propertyKeyIds;
        recordCursor.placeAt( firstPropertyId, FORCE );
        this.lock = lock;
        return this;
    }

    @Override
    public boolean next()
    {
        return fetched = fetchNext();
    }

    private boolean fetchNext()
    {
        while ( true )
        {
            // Are there more properties to return for this current record we're at?
            if ( payload.next() )
            {
                return true;
            }

            // No, OK continue down the chain and hunt for more...
            if ( recordCursor.next() )
            {
                // All good, we can get values off of this record
                PropertyRecord propertyRecord = recordCursor.get();
                payload.init( propertyKeyIds, propertyRecord.getBlocks(), propertyRecord.getNumberOfBlocks() );
                if ( payload.next() )
                {
                    return true;
                }
            }
            else if ( Record.NO_NEXT_PROPERTY.is( recordCursor.get().getNextProp() ) )
            {
                // No more records in this chain, i.e. no more properties.
                return false;
            }

            // Sort of alright, this record isn't in use, but could just be due to concurrent delete.
            // Continue to next record in the chain and try there.
        }
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
        if ( !fetched )
        {
            throw new IllegalStateException();
        }
        return this;
    }

    @Override
    public void close()
    {
        try
        {
            propertyKeyIds = null;
            fetched = false;
            payload.close();
            instanceCache.accept( this );
        }
        finally
        {
            lock.release();
            lock = null;
        }
    }
}
