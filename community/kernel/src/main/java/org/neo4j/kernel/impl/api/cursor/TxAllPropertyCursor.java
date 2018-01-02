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
package org.neo4j.kernel.impl.api.cursor;

import java.util.Iterator;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.util.VersionedHashMap;

/**
 * Overlays transaction state on a {@link PropertyItem} cursors.
 */
public class TxAllPropertyCursor extends TxAbstractPropertyCursor
{
    private Iterator<DefinedProperty> added;

    public TxAllPropertyCursor( Consumer<TxAbstractPropertyCursor> instanceCache )
    {
        super( instanceCache );
    }

    public Cursor<PropertyItem> init( Cursor<PropertyItem> cursor,
            VersionedHashMap<Integer, DefinedProperty> addedProperties,
            VersionedHashMap<Integer, DefinedProperty> changedProperties,
            VersionedHashMap<Integer, DefinedProperty> removedProperties )
    {
        this.cursor = cursor;
        this.addedProperties = addedProperties;
        this.changedProperties = changedProperties;
        this.removedProperties = removedProperties;

        return this;
    }

    @Override
    public boolean next()
    {
        if ( added == null )
        {
            while ( cursor.next() )
            {
                int propertyKeyId = cursor.get().propertyKeyId();

                if ( changedProperties != null )
                {
                    Property property = changedProperties.get( propertyKeyId );

                    if ( property != null )
                    {
                        this.property = (DefinedProperty) property;
                        return true;
                    }
                }

                if ( removedProperties == null || !removedProperties.containsKey( propertyKeyId ) )
                {
                    this.property = Property.property( propertyKeyId, cursor.get().value() );
                    return true;
                }
            }

            if ( addedProperties != null )
            {
                added = addedProperties.values().iterator();
            }

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
