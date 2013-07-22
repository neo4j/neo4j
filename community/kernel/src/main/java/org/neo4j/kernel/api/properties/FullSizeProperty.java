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
package org.neo4j.kernel.api.properties;

abstract class FullSizeProperty extends PropertyWithValue
{
    final long propertyKeyId;

    protected FullSizeProperty( long propertyKeyId )
    {
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public final long propertyKeyId()
    {
        return propertyKeyId;
    }

    @Override
    public final boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o != null && getClass() == o.getClass() )
        {
            FullSizeProperty that = (FullSizeProperty) o;
            return propertyKeyId == that.propertyKeyId && hasEqualValue( that );
        }
        return false;
    }

    @Override
    public final boolean isNoProperty()
    {
        return false;
    }

    @Override
    public final int hashCode()
    {
        return (int) (propertyKeyId ^ (propertyKeyId >>> 32)) ^ valueHash();
    }

    abstract int valueHash();

    abstract boolean hasEqualValue( FullSizeProperty that );
}
