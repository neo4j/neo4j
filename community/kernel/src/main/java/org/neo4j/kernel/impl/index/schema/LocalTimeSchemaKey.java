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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link LocalTimeValue}.
 */
class LocalTimeSchemaKey extends NativeSchemaKey
{
    static final int SIZE =
            Long.BYTES + /* nanoOfDay */
            Long.BYTES;  /* entityId */

    long nanoOfDay;

    @Override
    public void from( Value... values )
    {
        assertValidValue( values ).writeTo( this );
    }

    private LocalTimeValue assertValidValue( Value... values )
    {
        if ( values.length > 1 )
        {
            throw new IllegalArgumentException( "Tried to create composite key with non-composite schema key layout" );
        }
        if ( values.length < 1 )
        {
            throw new IllegalArgumentException( "Tried to create key without value" );
        }
        if ( !(values[0] instanceof LocalTimeValue) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support LocalTimeValue, tried to create key from " + values[0] );
        }
        return (LocalTimeValue) values[0];
    }

    @Override
    public Value asValue()
    {
        return LocalTimeValue.localTime( nanoOfDay );
    }

    @Override
    public void initValueAsLowest()
    {
        nanoOfDay = Long.MIN_VALUE;
    }

    @Override
    public void initValueAsHighest()
    {
        nanoOfDay = Long.MAX_VALUE;
    }

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link LocalTimeSchemaKey} to compare to.
     * @return comparison against the {@code other} {@link LocalTimeSchemaKey}.
     */
    int compareValueTo( LocalTimeSchemaKey other )
    {
        return Long.compare( nanoOfDay, other.nanoOfDay );
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,nanoOfDay=%s", asValue(), getEntityId(), nanoOfDay );
    }

    @Override
    public void writeLocalTime( long nanoOfDay )
    {
        this.nanoOfDay = nanoOfDay;
    }
}
