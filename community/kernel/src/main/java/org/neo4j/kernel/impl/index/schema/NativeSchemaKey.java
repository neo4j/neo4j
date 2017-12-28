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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueWriter;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * A value can be any {@link Number} and is represented as a {@code long} to store the raw bits and a type
 * to say if it's a long, double or float.
 *
 * Distinction between double and float exists because coersions between each other and long may differ.
 * TODO this should be figured out and potentially reduced to long, double types only.
 */
public abstract class NativeSchemaKey extends ValueWriter.Adapter<RuntimeException>
{
//    public static final int SIZE =
//            Byte.BYTES + /* type of value */
//            Long.BYTES + /* raw value bits */
//
//            // TODO this could use 6 bytes instead and have the highest 2 bits stored in the type byte
//            Long.BYTES;  /* entityId */

    public byte type;
    public long rawValueBits;
    public long entityId;

    /**
     * Marks that comparisons with this key requires also comparing entityId, this allows functionality
     * of inclusive/exclusive bounds of range queries.
     * This is because {@link GBPTree} only support from inclusive and to exclusive.
     * <p>
     * Note that {@code entityIdIsSpecialTieBreaker} is only an in memory state.
     */
    public boolean entityIdIsSpecialTieBreaker;

    public void from( long entityId, Value... values )
    {
        extractRawBitsAndType( assertValidValue( values ) );
        this.entityId = entityId;
        entityIdIsSpecialTieBreaker = false;
    }

    public abstract Value assertValidValue( Value... values );

    public String propertiesAsString()
    {
        return asValue().toString();
    }

    public abstract NumberValue asValue();

    public abstract void initAsLowest();

    public abstract void initAsHighest();
    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link NativeSchemaKey} to compare to.
     * @return comparison against the {@code other} {@link NativeSchemaKey}.
     */
    public int compareValueTo( NativeSchemaKey other )
    {
        return RawBits.compare( rawValueBits, type, other.rawValueBits, other.type );
    }

    /**
     * Extracts raw bits and type from a {@link Value} and store as state of this {@link NativeSchemaKey} instance.
     *
     * @param value actual {@link Value} value.
     */
    private void extractRawBitsAndType( Value value )
    {
        value.writeTo( this );
    }

    @Override
    public abstract String toString();
}
