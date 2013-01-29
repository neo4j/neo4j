/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.cache.SizeOfs;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;

public class LabelAsPropertyData implements PropertyData
{
    private final long id;
    private final int index;
    private Object value;

    public LabelAsPropertyData( long id, int index, Object value )
    {
        this.id = id;
        this.index = index;
        this.value = value;
    }
    
    @Override
    public int size()
    {
        return SizeOfs.withObjectOverhead( 4+8 );
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public int getIndex()
    {
        return index;
    }

    @Override
    public Object getValue()
    {
        return value;
    }

    @Override
    public void setNewValue( Object newValue )
    {
        this.value = newValue;
    }
    
    public static boolean representsLabel( PropertyData propertyData )
    {
        return propertyData instanceof LabelAsPropertyData;
    }
}
