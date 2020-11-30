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

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.values.storable.Value;

/**
 * Subclasses of this represent events related to property changes due to property or label addition, deletion or
 * update.
 * This is of use in populating indexes that might be relevant to node label and property combinations.
 *
 * @param <INDEX_KEY> {@link SchemaDescriptorSupplier} specifying the schema
 */
public abstract class IndexEntryUpdate<INDEX_KEY extends SchemaDescriptorSupplier>
{
    private final long entityId;
    private final UpdateMode updateMode;
    private final INDEX_KEY indexKey;

    IndexEntryUpdate( long entityId, INDEX_KEY indexKey, UpdateMode updateMode )
    {
        this.entityId = entityId;
        this.indexKey = indexKey;
        this.updateMode = updateMode;
    }

    public final long getEntityId()
    {
        return entityId;
    }

    public UpdateMode updateMode()
    {
        return updateMode;
    }

    public INDEX_KEY indexKey()
    {
        return indexKey;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        IndexEntryUpdate<?> that = (IndexEntryUpdate<?>) o;

        if ( entityId != that.entityId )
        {
            return false;
        }
        if ( updateMode != that.updateMode )
        {
            return false;
        }

        boolean schemaEquals = indexKey != null ? indexKey.schema().equals( that.indexKey.schema() ) : that.indexKey == null;
        if ( !schemaEquals )
        {
            return false;
        }

        return valueEquals( that );
    }

    @Override
    public int hashCode()
    {
        int result = (int) (entityId ^ (entityId >>> 32));
        result = 31 * result + (updateMode != null ? updateMode.hashCode() : 0);
        result = 31 * result + (indexKey != null ? indexKey.schema().hashCode() : 0);
        result = 31 * result + valueHash();
        return result;
    }

    public String describe( TokenNameLookup tokenNameLookup )
    {
        return String.format( getClass().getSimpleName() + "[id=%d, mode=%s, %s, %s]", entityId, updateMode,
                indexKey().schema().userDescription( tokenNameLookup ), valueToString() );
    }

    /**
     * Returns rough estimate of memory usage of this instance in bytes.
     */
    public abstract long roughSizeOfUpdate();

    /**
     * Equality check for values in sub-class.
     * Need to align with {@link #valueHash() value hash code}.
     */
    protected abstract boolean valueEquals( IndexEntryUpdate<?> that );

    /**
     * Hash code for values in sub-class.
     * Need to align with {@link #valueEquals(IndexEntryUpdate) value equals}.
     */
    protected abstract int valueHash();

    /**
     * Return string representation of value state.
     */
    protected abstract String valueToString();

    public static <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> add(
            long entityId, INDEX_KEY indexKey, Value... values )
    {
        return new ValueIndexEntryUpdate<>( entityId, indexKey, UpdateMode.ADDED, values );
    }

    public static <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> remove(
            long entityId, INDEX_KEY indexKey, Value... values )
    {
        return new ValueIndexEntryUpdate<>( entityId, indexKey, UpdateMode.REMOVED, values );
    }

    public static <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> change(
            long entityId, INDEX_KEY indexKey, Value before, Value after )
    {
        return new ValueIndexEntryUpdate<>( entityId, indexKey, UpdateMode.CHANGED,
                new Value[]{before}, new Value[]{after} );
    }

    public static <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> change(
            long entityId, INDEX_KEY indexKey, Value[] before, Value[] after )
    {
        return new ValueIndexEntryUpdate<>( entityId, indexKey, UpdateMode.CHANGED, before, after );
    }

    public static <INDEX_KEY extends SchemaDescriptorSupplier> TokenIndexEntryUpdate<INDEX_KEY> add(
            long entityId, INDEX_KEY indexKey, long[] values )
    {
        return new TokenIndexEntryUpdate<>( entityId, indexKey, UpdateMode.ADDED, values );
    }

    public static <INDEX_KEY extends SchemaDescriptorSupplier> TokenIndexEntryUpdate<INDEX_KEY> remove(
            long entityId, INDEX_KEY indexKey, long[] values )
    {
        return new TokenIndexEntryUpdate<>( entityId, indexKey, UpdateMode.REMOVED, values );
    }

    public static <INDEX_KEY extends SchemaDescriptorSupplier> TokenIndexEntryUpdate<INDEX_KEY> change(
            long entityId, INDEX_KEY indexKey, long[] before, long[] after )
    {
        return new TokenIndexEntryUpdate<>( entityId, indexKey, UpdateMode.CHANGED, before, after );
    }
}
