/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api.cursor;

import java.util.Iterator;
import java.util.function.Consumer;

import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.StorageProperty;

/**
 * Overlays transaction state on a {@link PropertyItem} cursors.
 */
public class TxAllPropertyCursor extends TxAbstractPropertyCursor
{
    private Iterator<StorageProperty> added;

    public TxAllPropertyCursor( Consumer<TxAbstractPropertyCursor> instanceCache )
    {
        super( instanceCache );
    }

    @Override
    public boolean next()
    {
        if ( added == null )
        {
            while ( cursor.next() )
            {
                int propertyKeyId = cursor.get().propertyKeyId();

                StorageProperty changedProperty = state.getChangedProperty( propertyKeyId );
                if ( changedProperty != null )
                {
                    this.property = changedProperty;
                    return true;
                }

                if ( !state.isPropertyRemoved( propertyKeyId ) )
                {
                    this.property = new PropertyKeyValue( propertyKeyId, cursor.get().value() );
                    return true;
                }
            }

            added = state.addedProperties();
        }

        if ( added != null && added.hasNext() )
        {
            property = added.next();
            return true;
        }
        else
        {
            property = null;
            return false;
        }
    }

    @Override
    public void close()
    {
        this.added = null;
        super.close();
    }
}
