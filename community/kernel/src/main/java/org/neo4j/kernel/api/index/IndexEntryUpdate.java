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

import org.neo4j.kernel.api.schema_new.SchemaUtil;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.index.UpdateMode;

import static java.lang.String.format;

/**
 * Subclasses of this represent events related to property changes due to property or label addition, deletion or
 * update.
 * This is of use in populating indexes that might be relevant to node label and property combinations.
 */
public class IndexEntryUpdate
{
    private final long entityId;
    private final UpdateMode updateMode;
    private final Object[] before;
    private Object[] values;
    private NewIndexDescriptor descriptor;

    private IndexEntryUpdate( long entityId, NewIndexDescriptor descriptor, UpdateMode updateMode, Object[] values )
    {
        this.entityId = entityId;
        this.descriptor = descriptor;
        this.before = null;
        this.values = values;
        this.updateMode = updateMode;
    }
    private IndexEntryUpdate( long entityId, NewIndexDescriptor descriptor, UpdateMode updateMode, Object[] before,
            Object[] values )
    {
        this.entityId = entityId;
        this.descriptor = descriptor;
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

    public NewIndexDescriptor descriptor()
    {
        return descriptor;
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
        if ( o != null && o instanceof IndexEntryUpdate )
        {
            IndexEntryUpdate other = (IndexEntryUpdate) o;
            return entityId == other.entityId &&
                    updateMode == other.updateMode &&
                    descriptor.equals( other.descriptor ) &&
                    Arrays.equals( values, other.values );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return ((((int)entityId * 31) +
                       updateMode.ordinal()) * 31 +
                       Arrays.hashCode( values )) * 31 + descriptor.hashCode();
    }

    @Override
    public String toString()
    {
        return format( "IndexEntryUpdate[id=%d, mode=%s, %s, values=%s]", entityId, updateMode, descriptor()
                .userDescription( SchemaUtil.idTokenNameLookup ), Arrays.toString(values) );
    }

    public static IndexEntryUpdate add( long nodeId, NewIndexDescriptor descriptor, Object... values )
    {
        return new IndexEntryUpdate( nodeId, descriptor, UpdateMode.ADDED, values );
    }

    public static IndexEntryUpdate remove( long nodeId, NewIndexDescriptor descriptor, Object... values )
    {
        return new IndexEntryUpdate( nodeId, descriptor, UpdateMode.REMOVED, values );
    }

    public static IndexEntryUpdate change( long nodeId, NewIndexDescriptor descriptor, Object before, Object after )
    {
        return new IndexEntryUpdate( nodeId, descriptor, UpdateMode.CHANGED, new Object[]{before}, new Object[]{after} );
    }

    public static IndexEntryUpdate change( long nodeId, NewIndexDescriptor descriptor, Object[] before, Object[] after )
    {
        return new IndexEntryUpdate( nodeId, descriptor, UpdateMode.CHANGED, before, after );
    }

    public Object[] beforeValues()
    {
        if( before == null )
        {
            throw new UnsupportedOperationException( "beforeValues is only valid for `UpdateMode.CHANGED" );
        }
        return before;
    }
}
