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

public class TokenIndexEntryUpdate<INDEX_KEY extends SchemaDescriptorSupplier> extends IndexEntryUpdate<INDEX_KEY>
{
    private final long[] before;
    private final long[] values;

    TokenIndexEntryUpdate( long entityId, INDEX_KEY index_key, UpdateMode updateMode, long[] values )
    {
        this( entityId, index_key, updateMode, null, values );
    }

    TokenIndexEntryUpdate( long entityId, INDEX_KEY index_key, UpdateMode updateMode, long[] before, long[] values )
    {
        super( entityId, index_key, updateMode );
        this.before = before;
        this.values = values;
    }

    public long[] values()
    {
        return values;
    }

    public long[] beforeValues()
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
        return HeapEstimator.sizeOf( values ) + (updateMode() == UpdateMode.CHANGED ? HeapEstimator.sizeOf( before ) : 0);
    }

    @Override
    protected boolean valueEquals( IndexEntryUpdate<?> o )
    {
        if ( !(o instanceof TokenIndexEntryUpdate) )
        {
            return false;
        }
        TokenIndexEntryUpdate<?> that = (TokenIndexEntryUpdate<?>) o;
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
}
