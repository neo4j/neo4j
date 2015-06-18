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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Cursor for all properties on a node or relationship.
 */
public class StorePropertyCursor implements PropertyCursor
{
    private PropertyStore propertyStore;
    private StoreStatement storeStatement;
    private InstanceCache<StorePropertyCursor> instanceCache;
    private PropertyRecordCursor propertyRecordCursor;
    private PropertyBlockCursor propertyBlockCursor;

    public StorePropertyCursor( PropertyStore propertyStore,
            StoreStatement storeStatement,
            InstanceCache<StorePropertyCursor> instanceCache )
    {
        this.propertyStore = propertyStore;
        this.storeStatement = storeStatement;
        this.instanceCache = instanceCache;
    }

    public StorePropertyCursor init( long firstPropertyId )
    {
        propertyRecordCursor = propertyStore.getPropertyRecordCursor( propertyRecordCursor, firstPropertyId );
        propertyBlockCursor = null;
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

    @Override
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
    public DefinedProperty getProperty()
    {
        return propertyBlockCursor.getProperty();
    }

    @Override
    public void close()
    {
        instanceCache.accept( this );
    }
}
