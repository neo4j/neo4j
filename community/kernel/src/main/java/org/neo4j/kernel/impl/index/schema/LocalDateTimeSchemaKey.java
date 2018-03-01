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

import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueWriter;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link LocalDateTimeValue}.
 */
class LocalDateTimeSchemaKey extends ValueWriter.Adapter<RuntimeException> implements NativeSchemaKey
{
    static final int SIZE =
            Long.BYTES +    /* epochSecond */
            Integer.BYTES + /* nanoOfSecond */
            Long.BYTES;     /* entityId */

    private long entityId;
    private boolean compareId;

    int nanoOfSecond;
    long epochSecond;

    public void setCompareId( boolean compareId )
    {
        this.compareId = compareId;
    }

    public boolean getCompareId()
    {
        return compareId;
    }

    @Override
    public long getEntityId()
    {
        return entityId;
    }

    @Override
    public void setEntityId( long entityId )
    {
        this.entityId = entityId;
    }

    @Override
    public void from( long entityId, Value... values )
    {
        this.entityId = entityId;
        compareId = false;
        assertValidValue( values ).writeTo( this );
    }

    private LocalDateTimeValue assertValidValue( Value... values )
    {
        if ( values.length > 1 )
        {
            throw new IllegalArgumentException( "Tried to create composite key with non-composite schema key layout" );
        }
        if ( values.length < 1 )
        {
            throw new IllegalArgumentException( "Tried to create key without value" );
        }
        if ( !(values[0] instanceof LocalDateTimeValue) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support LocalDateTimeValue, tried to create key from " + values[0] );
        }
        return (LocalDateTimeValue) values[0];
    }

    @Override
    public String propertiesAsString()
    {
        return asValue().toString();
    }

    @Override
    public Value asValue()
    {
        return LocalDateTimeValue.localDateTime( epochSecond, nanoOfSecond );
    }

    @Override
    public void initAsLowest()
    {
        epochSecond = Long.MIN_VALUE;
        nanoOfSecond = Integer.MIN_VALUE;
        entityId = Long.MIN_VALUE;
        compareId = true;
    }

    @Override
    public void initAsHighest()
    {
        epochSecond = Long.MAX_VALUE;
        nanoOfSecond = Integer.MAX_VALUE;
        entityId = Long.MAX_VALUE;
        compareId = true;
    }

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link LocalDateTimeSchemaKey} to compare to.
     * @return comparison against the {@code other} {@link LocalDateTimeSchemaKey}.
     */
    int compareValueTo( LocalDateTimeSchemaKey other )
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
        return format( "value=%s,entityId=%d,nanoOfDay=%s", asValue(), entityId, nanoOfSecond );
    }

    @Override
    public void writeLocalDateTime( long epochSecond, int nano )
    {
        this.nanoOfSecond = nano;
        this.epochSecond = epochSecond;
    }
}
