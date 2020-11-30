/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api;

import java.util.Arrays;

import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

public class ValueIndexEntryUpdate<INDEX_KEY extends SchemaDescriptorSupplier> extends IndexEntryUpdate<INDEX_KEY>
{
    private final Value[] before;
    private final Value[] values;

    ValueIndexEntryUpdate( long entityId, INDEX_KEY index_key, UpdateMode updateMode, Value[] values )
    {
        this( entityId, index_key, updateMode, null, values );
    }

    ValueIndexEntryUpdate( long entityId, INDEX_KEY indexKey, UpdateMode updateMode, Value[] before, Value[] values )
    {
        super( entityId, indexKey, updateMode );
        validateValuesLength( indexKey, before, values );

        this.before = before;
        this.values = values;
    }

    public Value[] values()
    {
        return values;
    }

    public Value[] beforeValues()
    {
        if ( before == null )
        {
            throw new UnsupportedOperationException( "beforeValues is only valid for `UpdateMode.CHANGED" );
        }
        return before;
    }

    @Override
    public long roughSizeOfUpdate()
    {
        return heapSizeOf( values ) + (updateMode() == UpdateMode.CHANGED ? heapSizeOf( before ) : 0);
    }

    @Override
    protected boolean valueEquals( IndexEntryUpdate<?> o )
    {
        if ( !(o instanceof ValueIndexEntryUpdate) )
        {
            return false;
        }
        ValueIndexEntryUpdate<?> that = (ValueIndexEntryUpdate<?>) o;
        if ( !Arrays.equals( before, that.before ) )
        {
            return false;
        }
        return Arrays.equals( values, that.values );
    }

    @Override
    protected int valueHash()
    {
        int result = Arrays.hashCode( before );
        result = 31 * result + Arrays.hashCode( values );
        return result;
    }

    @Override
    protected String valueToString()
    {
        return String.format( "beforeValues=%s, values=%s", Arrays.toString( before ), Arrays.toString( values ) );
    }

    private static void validateValuesLength( SchemaDescriptorSupplier indexKey, Value[] before, Value[] values )
    {
        // we do not support partial index entries
        assert indexKey.schema().getPropertyIds().length == values.length :
                format( "ValueIndexEntryUpdate values must be of same length as index compositeness. " +
                        "Index on %s, but got values %s", indexKey.schema().toString(), Arrays.toString( values ) );
        assert before == null || before.length == values.length;
    }
    private static long heapSizeOf( Value[] values )
    {
        long size = 0;
        if ( values != null )
        {
            for ( Value value : values )
            {
                if ( value != null )
                {
                    size += heapSizeOf( value );
                }
            }
        }
        return size;
    }

    private static long heapSizeOf( Value value )
    {
        return HeapEstimator.sizeOf( value.asObject() );
    }
}
