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

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.kernel.impl.store.TemporalValueWriterAdapter;
import org.neo4j.values.storable.Value;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * This is the abstraction of what NativeSchemaIndex with friends need from a schema key.
 * Note that it says nothing about how keys are compared, serialized, read, written, etc. That is the job of Layout.
 */
abstract class NativeSchemaKey<SELF extends NativeSchemaKey<SELF>> extends TemporalValueWriterAdapter<RuntimeException>
{
    private static final boolean DEFAULT_COMPARE_ID = true;

    private long entityId;
    private boolean compareId = DEFAULT_COMPARE_ID;

    /**
     * Marks that comparisons with this key requires also comparing entityId, this allows functionality
     * of inclusive/exclusive bounds of range queries.
     * This is because {@link GBPTree} only support from inclusive and to exclusive.
     * <p>
     * Note that {@code compareId} is only an in memory state.
     */
    void setCompareId( boolean compareId )
    {
        this.compareId = compareId;
    }

    boolean getCompareId()
    {
        return compareId;
    }

    long getEntityId()
    {
        return entityId;
    }

    void setEntityId( long entityId )
    {
        this.entityId = entityId;
    }

    final void from( long entityId, Value... values )
    {
        initialize( entityId );
        // copy value state and store in this key instance
        assertValidValue( values ).writeTo( this );
    }

    /**
     * Initializes this key with entity id and resets other flags to default values.
     * Doesn't touch value data.
     *
     * @param entityId entity id to set for this key.
     */
    void initialize( long entityId )
    {
        this.compareId = DEFAULT_COMPARE_ID;
        this.entityId = entityId;
    }

    private Value assertValidValue( Value... values )
    {
        if ( values.length > 1 )
        {
            throw new IllegalArgumentException( "Tried to create composite key with non-composite schema key layout" );
        }
        if ( values.length < 1 )
        {
            throw new IllegalArgumentException( "Tried to create key without value" );
        }
        return assertCorrectType( values[0] );
    }

    protected abstract Value assertCorrectType( Value value );

    String propertiesAsString()
    {
        return asValue().toString();
    }

    abstract Value asValue();

    final void initAsLowest()
    {
        initialize( Long.MIN_VALUE );
        initValueAsLowest();
    }

    abstract void initValueAsLowest();

    final void initAsHighest()
    {
        initialize( Long.MAX_VALUE );
        initValueAsHighest();
    }

    abstract void initValueAsHighest();

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the key to compare to.
     * @return comparison against the {@code other} key.
     */
    abstract int compareValueTo( SELF other );
}
