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
package org.neo4j.kernel.impl.api.cursor;

import java.util.Iterator;
import java.util.function.Consumer;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
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
        property = null;
        if ( added == null )
        {
            while ( cursor.next() )
            {
                int propertyKeyId = cursor.get().propertyKeyId();

                if ( state.isPropertyRemoved( propertyKeyId ) )
                {
                    continue;
                }

                if ( property == null )
                {
                    this.property = (DefinedProperty) state.getChangedProperty( propertyKeyId );
                }

                if( property == null )
                {
                    this.property = Property.property( propertyKeyId, cursor.get().value() );
                }

                return property != null;
            }

            added = state.addedProperties();
        }

        if ( added != null && added.hasNext() )
        {
            property = (DefinedProperty) added.next();
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
        super.close();
        this.added = null;
    }
}
