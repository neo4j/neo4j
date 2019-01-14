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
package org.neo4j.values.storable;

import org.neo4j.values.ValueMapper;

import static java.lang.String.format;

/**
 * This does not extend AbstractProperty since the JVM can take advantage of the 4 byte initial field alignment if
 * we don't extend a class that has fields.
 */
public final class ShortValue extends IntegralValue
{
    private final short value;

    ShortValue( short value )
    {
        this.value = value;
    }

    public short value()
    {
        return value;
    }

    @Override
    public long longValue()
    {
        return value;
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeInteger( value );
    }

    @Override
    public Short asObjectCopy()
    {
        return value;
    }

    @Override
    public String prettyPrint()
    {
        return Short.toString( value );
    }

    @Override
    public String toString()
    {
        return format( "%s(%d)", getTypeName(), value );
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapShort( this );
    }

    @Override
    public String getTypeName()
    {
        return "Short";
    }
}
