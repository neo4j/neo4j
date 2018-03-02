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

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueWriter;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link String},
 * or rather any string that {@link GBPTree} can handle.
 */
class DateSchemaKey extends ValueWriter.Adapter<RuntimeException> implements NativeSchemaKey
{
    static final int SIZE =
            Long.BYTES + /* raw value bits */
            Long.BYTES;  /* entityId */

    private long entityId;
    private boolean compareId;

    long epochDay;

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

    private DateValue assertValidValue( Value... values )
    {
        if ( values.length > 1 )
        {
            throw new IllegalArgumentException( "Tried to create composite key with non-composite schema key layout" );
        }
        if ( values.length < 1 )
        {
            throw new IllegalArgumentException( "Tried to create key without value" );
        }
        if ( !(values[0] instanceof DateValue) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support DateValue, tried to create key from " + values[0] );
        }
        return (DateValue) values[0];
    }

    @Override
    public String propertiesAsString()
    {
        return asValue().toString();
    }

    @Override
    public Value asValue()
    {
        return DateValue.epochDate( epochDay );
    }

    @Override
    public void initAsLowest()
    {
        epochDay = Long.MIN_VALUE;
        entityId = Long.MIN_VALUE;
        compareId = true;
    }

    @Override
    public void initAsHighest()
    {
        epochDay = Long.MAX_VALUE;
        entityId = Long.MAX_VALUE;
        compareId = true;
    }

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link DateSchemaKey} to compare to.
     * @return comparison against the {@code other} {@link DateSchemaKey}.
     */
    int compareValueTo( DateSchemaKey other )
    {
        return Long.compare( epochDay, other.epochDay );
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,epochDay=%s", asValue(), entityId, epochDay );
    }

    @Override
    public void writeDate( long epochDay )
    {
        this.epochDay = epochDay;
    }
}
