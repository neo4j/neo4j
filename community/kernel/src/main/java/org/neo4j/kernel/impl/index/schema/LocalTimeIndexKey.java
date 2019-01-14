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

import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link LocalTimeValue}.
 */
class LocalTimeIndexKey extends NativeIndexSingleValueKey<LocalTimeIndexKey>
{
    static final int SIZE =
            Long.BYTES + /* nanoOfDay */
            ENTITY_ID_SIZE;  /* entityId */

    long nanoOfDay;

    @Override
    public Value asValue()
    {
        return LocalTimeValue.localTime( nanoOfDay );
    }

    @Override
    public void initValueAsLowest( ValueGroup valueGroups )
    {
        nanoOfDay = Long.MIN_VALUE;
    }

    @Override
    public void initValueAsHighest( ValueGroup valueGroups )
    {
        nanoOfDay = Long.MAX_VALUE;
    }

    @Override
    public int compareValueTo( LocalTimeIndexKey other )
    {
        return Long.compare( nanoOfDay, other.nanoOfDay );
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,nanoOfDay=%d", asValue(), getEntityId(), nanoOfDay );
    }

    @Override
    public void writeLocalTime( long nanoOfDay )
    {
        this.nanoOfDay = nanoOfDay;
    }

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( !(value instanceof LocalTimeValue) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support LocalTimeValue, tried to create key from " + value );
        }
        return value;
    }
}
