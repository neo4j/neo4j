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

import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyDatas;

final class BigLongProperty extends FullSizeProperty
{
    private final long value;

    BigLongProperty( long propertyKeyId, long value )
    {
        super( propertyKeyId );
        this.value = value;
    }

    @Override
    public boolean valueEquals( Object other )
    {
        if ( other instanceof Long )
        {
            return value == (long)other;
        }

        return valueCompare( value, other );
    }

    @Override
    public Long value()
    {
        return value;
    }

    @Override
    public int intValue()
    {
        throw new ClassCastException( String.format( "[%s:long] is not small enough to fit into an int.", value ) );
    }

    @Override
    public long longValue()
    {
        return value;
    }

    @Override
    int valueHash()
    {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    boolean hasEqualValue( FullSizeProperty that )
    {
        return value == ((BigLongProperty) that).value;
    }

    @Override
    @Deprecated
    public PropertyData asPropertyDataJustForIntegration()
    {
        return PropertyDatas.forLong( (int) propertyKeyId, -1, value );
    }
}
