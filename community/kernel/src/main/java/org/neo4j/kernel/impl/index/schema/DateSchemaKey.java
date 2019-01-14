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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link DateValue}.
 */
class DateSchemaKey extends NativeSchemaKey<DateSchemaKey>
{
    static final int SIZE =
            Long.BYTES + /* epochDay */
            Long.BYTES;  /* entityId */

    long epochDay;

    @Override
    public Value asValue()
    {
        return DateValue.epochDate( epochDay );
    }

    @Override
    void initValueAsLowest()
    {
        epochDay = Long.MIN_VALUE;
    }

    @Override
    void initValueAsHighest()
    {
        epochDay = Long.MAX_VALUE;
    }

    @Override
    public int compareValueTo( DateSchemaKey other )
    {
        return Long.compare( epochDay, other.epochDay );
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,epochDay=%d", asValue(), getEntityId(), epochDay );
    }

    @Override
    public void writeDate( long epochDay )
    {
        this.epochDay = epochDay;
    }

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( !(value instanceof DateValue) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support DateValue, tried to create key from " + value );
        }
        return value;
    }
}
