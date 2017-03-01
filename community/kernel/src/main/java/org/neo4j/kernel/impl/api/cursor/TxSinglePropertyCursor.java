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

import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;

/**
 * Overlays transaction state on a {@link PropertyItem} cursors.
 */
public class TxSinglePropertyCursor extends TxAbstractPropertyCursor
{
    private int propertyKeyId;

    public TxSinglePropertyCursor( Consumer<TxAbstractPropertyCursor> instanceCache )
    {
        super( instanceCache );
    }

    public Cursor<PropertyItem> init( Cursor<PropertyItem> cursor,
            PropertyContainerState state, int propertyKeyId )
    {
        super.init( cursor, state );
        this.propertyKeyId = propertyKeyId;
        return this;
    }

    @Override
    public boolean next()
    {
        property = null;

        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return false;
        }

        try
        {
            if ( state.isPropertyRemoved( propertyKeyId ) )
            {
                return false;
            }

            if ( property == null )
            {
                property = (DefinedProperty) state.getChangedProperty( propertyKeyId );
            }

            if ( property == null )
            {
                property = (DefinedProperty) state.getAddedProperty( propertyKeyId );
            }

            if ( property == null && cursor.next() )
            {
                property = Property.property( cursor.get().propertyKeyId(), cursor.get().value() );
            }

            return property != null;
        }
        finally
        {
            propertyKeyId = StatementConstants.NO_SUCH_PROPERTY_KEY;
        }
    }

    @Override
    public PropertyItem get()
    {
        if ( property == null )
        {
            throw new IllegalStateException();
        }

        return super.get();
    }
}
