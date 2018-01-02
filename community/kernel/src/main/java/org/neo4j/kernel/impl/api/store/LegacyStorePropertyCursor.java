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

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Cursor for all properties on a node or relationship.
 * <p>
 * This implementation uses the old PropertyBlock loading, which eager loads
 * all data on init.
 */
public class LegacyStorePropertyCursor implements Cursor<PropertyItem>, PropertyItem
{
    private PropertyStore propertyStore;
    private InstanceCache<LegacyStorePropertyCursor> instanceCache;
    private PropertyRecordCursor propertyRecordCursor;
    private PropertyBlockCursor propertyBlockCursor;

    public LegacyStorePropertyCursor( PropertyStore propertyStore,
            InstanceCache<LegacyStorePropertyCursor> instanceCache )
    {
        this.propertyStore = propertyStore;
        this.instanceCache = instanceCache;
    }

    public LegacyStorePropertyCursor init( long firstPropertyId )
    {
        propertyRecordCursor = propertyStore.getPropertyRecordCursor( propertyRecordCursor, firstPropertyId );
        propertyBlockCursor = null;
        return this;
    }

    @Override
    public PropertyItem get()
    {
        return this;
    }

    @Override
    public boolean next()
    {
        if ( propertyBlockCursor != null && propertyBlockCursor.next() )
        {
            return true;
        }
        else if ( propertyRecordCursor.next() )
        {
            propertyBlockCursor = propertyRecordCursor.getPropertyBlockCursor( propertyBlockCursor );

            return propertyBlockCursor.next();
        }

        return false;
    }

    public boolean seek( int keyId )
    {
        while ( next() )
        {
            if ( propertyBlockCursor.getPropertyBlock().getKeyIndexId() == keyId )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public int propertyKeyId()
    {
        return propertyBlockCursor.getPropertyBlock().getKeyIndexId();
    }

    @Override
    public Object value()
    {
        return propertyBlockCursor.getProperty().value();
    }

    @Override
    public void close()
    {
        instanceCache.accept( this );
    }
}
