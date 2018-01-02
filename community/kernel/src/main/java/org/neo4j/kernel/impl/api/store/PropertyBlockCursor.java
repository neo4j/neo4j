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

import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;

/**
 * Cursor over a set of {@link PropertyBlock} instances.
 *
 * This is used by the {@link LegacyStorePropertyCursor} to find
 * all properties of a node.
 */
public class PropertyBlockCursor
{
    private PropertyBlock[] blockRecords;
    private PropertyBlock current;
    private int blockRecordsCursor;
    private int index;
    private Supplier<PropertyStore> propertyStore;

    public PropertyBlockCursor( PropertyStore propertyStore )
    {
        this.propertyStore = Suppliers.singleton(propertyStore);
    }

    public void init( PropertyBlock[] blockRecords, int blockRecordsCursor )
    {
        this.blockRecords = blockRecords;
        this.blockRecordsCursor = blockRecordsCursor;
        index = 0;
    }

    public boolean next()
    {
        if (index < blockRecordsCursor)
        {
            current = blockRecords[index];
            index++;
            return true;
        } else
        {
            return false;
        }
    }

    public PropertyBlock getPropertyBlock()
    {
        return current;
    }

    public void close()
    {

    }

    public DefinedProperty getProperty()
    {
        return current.newPropertyData( propertyStore );
    }

    public Object getValue()
    {
        return current.getType().getValue( current, propertyStore.get() );
    }
}
