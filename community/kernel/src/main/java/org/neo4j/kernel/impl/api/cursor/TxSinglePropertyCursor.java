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

import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;

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
            PropertyContainerState state, int propertyKeyId )
    {
        super.init( cursor, state );
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
            StorageProperty changedProperty = state.getChangedProperty( propertyKeyId );
            if ( changedProperty != null )
            {
                this.property = changedProperty;
                return true;
            }

            StorageProperty addedProperty = state.getAddedProperty( propertyKeyId );
            if ( addedProperty != null )
            {
                this.property = addedProperty;
                return true;
            }

            if ( state.isPropertyRemoved( propertyKeyId ) )
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
            property = new PropertyKeyValue( cursor.get().propertyKeyId(), cursor.get().value() );
        }
        seekFoundIt = false;

        return super.get();
    }
}
