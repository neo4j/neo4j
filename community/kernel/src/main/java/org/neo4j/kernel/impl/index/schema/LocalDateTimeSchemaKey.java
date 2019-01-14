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

import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link LocalDateTimeValue}.
 */
class LocalDateTimeSchemaKey extends NativeSchemaKey<LocalDateTimeSchemaKey>
{
    static final int SIZE =
            Long.BYTES +    /* epochSecond */
            Integer.BYTES + /* nanoOfSecond */
            Long.BYTES;     /* entityId */

    int nanoOfSecond;
    long epochSecond;

    @Override
    public Value asValue()
    {
        return LocalDateTimeValue.localDateTime( epochSecond, nanoOfSecond );
    }

    @Override
    public void initValueAsLowest()
    {
        epochSecond = Long.MIN_VALUE;
        nanoOfSecond = Integer.MIN_VALUE;
    }

    @Override
    public void initValueAsHighest()
    {
        epochSecond = Long.MAX_VALUE;
        nanoOfSecond = Integer.MAX_VALUE;
    }

    @Override
    public int compareValueTo( LocalDateTimeSchemaKey other )
    {
        int compare = Long.compare( epochSecond, other.epochSecond );
        if ( compare == 0 )
        {
            compare = Integer.compare( nanoOfSecond, other.nanoOfSecond );
        }
        return compare;
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,epochSecond=%d,nanoOfSecond=%d",
                        asValue(), getEntityId(), epochSecond, nanoOfSecond );
    }

    @Override
    public void writeLocalDateTime( long epochSecond, int nano )
    {
        this.nanoOfSecond = nano;
        this.epochSecond = epochSecond;
    }

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( !(value instanceof LocalDateTimeValue) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support LocalDateTimeValue, tried to create key from " + value );
        }
        return value;
    }
}
