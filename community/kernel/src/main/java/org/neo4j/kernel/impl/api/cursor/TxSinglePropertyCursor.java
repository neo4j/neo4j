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

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.util.VersionedHashMap;

/**
 * Overlays transaction state on a {@link PropertyItem} cursors.
 */
public class TxSinglePropertyCursor extends TxAbstractPropertyCursor
{
    private int propertyKeyId;
    private boolean seekFoundIt;

    public TxSinglePropertyCursor( Consumer<TxAbstractPropertyCursor> instanceCache )
    {
        super( instanceCache );
    }

    public Cursor<PropertyItem> init( Cursor<PropertyItem> cursor,
            VersionedHashMap<Integer, DefinedProperty> addedProperties,
            VersionedHashMap<Integer, DefinedProperty> changedProperties,
            VersionedHashMap<Integer, DefinedProperty> removedProperties,
            int propertyKeyId )
    {
        super.init( cursor, addedProperties, changedProperties, removedProperties );
        this.propertyKeyId = propertyKeyId;

        return this;
    }

    @Override
    public boolean next()
    {
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return false;
        }

        try
        {
            seekFoundIt = false;
            if ( changedProperties != null )
            {
                Property property = changedProperties.get( propertyKeyId );

                if ( property != null )
                {
                    this.property = (DefinedProperty) property;
                    return true;
                }
            }

            if ( addedProperties != null )
            {
                Property property = addedProperties.get( propertyKeyId );

                if ( property != null )
                {
                    this.property = (DefinedProperty) property;
                    return true;
                }
            }

            if ( removedProperties != null && removedProperties.containsKey( propertyKeyId ) )
            {
                this.property = null;
                return false;
            }

            if ( cursor.next() )
            {
                this.property = null;
                return seekFoundIt = true;
            }
            else
            {
                this.property = null;
                return false;
            }
        }
        finally
        {
            propertyKeyId = StatementConstants.NO_SUCH_PROPERTY_KEY;
        }
    }

    @Override
    public PropertyItem get()
    {
        if ( property == null && !seekFoundIt )
        {
            throw new IllegalStateException();
        }

        if ( seekFoundIt )
        {
            property = Property.property( cursor.get().propertyKeyId(), cursor.get().value() );
        }
        seekFoundIt = false;

        return super.get();
    }
}
