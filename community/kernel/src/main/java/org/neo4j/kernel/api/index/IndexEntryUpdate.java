/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import java.util.Arrays;

import org.neo4j.kernel.api.schema.LabelSchemaSupplier;
import org.neo4j.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.impl.api.index.UpdateMode;

import static java.lang.String.format;

/**
 * Subclasses of this represent events related to property changes due to property or label addition, deletion or
 * update.
 * This is of use in populating indexes that might be relevant to node label and property combinations.
 */
public class IndexEntryUpdate<INDEX_KEY extends LabelSchemaSupplier>
{
    private final long entityId;
    private final UpdateMode updateMode;
    private final Object[] before;
    private final Object[] values;
    private final INDEX_KEY indexKey;

    private IndexEntryUpdate( long entityId, INDEX_KEY indexKey, UpdateMode updateMode, Object... values )
    {
        this( entityId, indexKey, updateMode, null, values );
    }

    private IndexEntryUpdate( long entityId, INDEX_KEY indexKey, UpdateMode updateMode, Object[] before,
            Object[] values )
    {
        // we do not support partial index entries
        assert indexKey.schema().getPropertyIds().length == values.length :
                format( "IndexEntryUpdate values must be of same length as index compositness. " +
                        "Index on %s, but got values %s", indexKey.schema().toString(), Arrays.toString( values ) );
        assert before == null || before.length == values.length;

        this.entityId = entityId;
        this.indexKey = indexKey;
        this.before = before;
        this.values = values;
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

    public Object[] values()
    {
        return values;
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

        IndexEntryUpdate that = (IndexEntryUpdate) o;

        if ( entityId != that.entityId )
        {
            return false;
        }
        if ( updateMode != that.updateMode )
        {
            return false;
        }
        if ( !Arrays.deepEquals( before, that.before ) )
        {
            return false;
        }
        if ( !Arrays.deepEquals( values, that.values ) )
        {
            return false;
        }
        return indexKey != null ? indexKey.schema().equals( that.indexKey.schema() ) : that.indexKey == null;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (entityId ^ (entityId >>> 32));
        result = 31 * result + (updateMode != null ? updateMode.hashCode() : 0);
        result = 31 * result + Arrays.deepHashCode( before );
        result = 31 * result + Arrays.deepHashCode( values );
        result = 31 * result + (indexKey != null ? indexKey.schema().hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return format( "IndexEntryUpdate[id=%d, mode=%s, %s, values=%s]", entityId, updateMode, indexKey().schema()
                .userDescription( SchemaUtil.idTokenNameLookup ), Arrays.toString(values) );
    }

    public static <INDEX_KEY extends LabelSchemaSupplier> IndexEntryUpdate<INDEX_KEY> add(
            long nodeId, INDEX_KEY indexKey, Object... values )
    {
        return new IndexEntryUpdate<>( nodeId, indexKey, UpdateMode.ADDED, values );
    }

    public static <INDEX_KEY extends LabelSchemaSupplier> IndexEntryUpdate<INDEX_KEY> remove(
            long nodeId, INDEX_KEY indexKey, Object... values )
    {
        return new IndexEntryUpdate<>( nodeId, indexKey, UpdateMode.REMOVED, values );
    }

    public static <INDEX_KEY extends LabelSchemaSupplier> IndexEntryUpdate<INDEX_KEY> change(
            long nodeId, INDEX_KEY indexKey, Object before, Object after )
    {
        return new IndexEntryUpdate<>( nodeId, indexKey, UpdateMode.CHANGED,
                new Object[]{before}, new Object[]{after} );
    }

    public static <INDEX_KEY extends LabelSchemaSupplier> IndexEntryUpdate<INDEX_KEY> change(
            long nodeId, INDEX_KEY indexKey, Object[] before, Object[] after )
    {
        return new IndexEntryUpdate<>( nodeId, indexKey, UpdateMode.CHANGED, before, after );
    }

    public Object[] beforeValues()
    {
        if ( before == null )
        {
            throw new UnsupportedOperationException( "beforeValues is only valid for `UpdateMode.CHANGED" );
        }
        return before;
    }
}
