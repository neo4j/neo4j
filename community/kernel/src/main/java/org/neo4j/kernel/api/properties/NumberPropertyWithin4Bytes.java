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

/**
 * This does not extend AbstractProperty since the JVM can take advantage of the 4 byte initial field alignment if
 * we don't extend a class that has fields.
 */
abstract class NumberPropertyWithin4Bytes extends PropertyWithValue
{
    @Override
    public int hashCode()
    {
        long propertyKeyId = propertyKeyId();
        return valueBits() ^ (int) (propertyKeyId ^ (propertyKeyId >>> 32));
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == this )
        {
            return true;
        }
        if ( obj == null || obj.getClass() != this.getClass() )
        {
            return false;
        }
        NumberPropertyWithin4Bytes that = (NumberPropertyWithin4Bytes) obj;
        return this.propertyKeyId() == that.propertyKeyId() && hasEqualValue( that );
    }

    @Override
    final public boolean isNoProperty()
    {
        return false;
    }

    abstract boolean hasEqualValue( NumberPropertyWithin4Bytes that );

    int valueBits()
    {
        return intValue();
    }

    @Override
    public abstract Number value();

    @Override
    public Number numberValue()
    {
        return value();
    }

    @Override
    public Number numberValue( Number defaultValue )
    {
        return value();
    }
}
