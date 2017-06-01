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

import org.neo4j.values.Value;
import org.neo4j.index.internal.gbptree.GBPTree;

import static org.neo4j.kernel.impl.index.schema.NumberValueConversion.assertValidSingleNumber;

/**
 * Includes comparison value and entity id (to be able to handle non-unique values).
 * Comparison value is basically any number as a double, a conversion which is lossy by nature,
 * especially for higher decimal values. Actual value is stored in {@link NumberValue}
 * for ability to filter accidental coersions directly internally.
 */
class NumberKey
{
    static final int SIZE =
            Long.SIZE + /* compare value (double represented by long) */
            Long.SIZE;  /* entityId */

    double value;
    long entityId;

    /**
     * Marks that comparisons with this key requires also comparing entityId, this allows functionality
     * of inclusive/exclusive bounds of range queries.
     * This is because {@link GBPTree} only support from inclusive and to exclusive.
     * <p>
     * Note that {@code entityIdIsSpecialTieBreaker} is only an in memory state.
     */
    boolean entityIdIsSpecialTieBreaker;

    public void from( long entityId, Value[] values )
    {
        this.value = assertValidSingleNumber( values ).doubleValue();
        this.entityId = entityId;
        entityIdIsSpecialTieBreaker = false;
    }

    String propertiesAsString()
    {
        return String.valueOf( value );
    }

    void initAsLowest()
    {
        value = Double.NEGATIVE_INFINITY;
        entityId = Long.MIN_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    void initAsHighest()
    {
        value = Double.POSITIVE_INFINITY;
        entityId = Long.MAX_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    @Override
    public String toString()
    {
        return "compareValue=" + value + ",entityId=" + entityId;
    }
}
