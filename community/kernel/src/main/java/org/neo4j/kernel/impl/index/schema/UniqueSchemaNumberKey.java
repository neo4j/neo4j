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

import static org.neo4j.kernel.impl.index.schema.SchemaNumberValueConversion.assertValidSingleNumberPropertyValue;

/**
 * Contains only comparison value, which means that all values needs to be unique.
 * Comparison value is basically any number as a double, a conversion which is lossy by nature,
 * especially for higher decimal values. Actual value is stored in {@link SchemaNumberValue}
 * for ability to filter accidental coersions directly internally.
 */
class UniqueSchemaNumberKey implements SchemaNumberKey
{
    static final int SIZE = Long.SIZE;

    double value;
    boolean isHighest;

    @Override
    public void from( long entityId, Object[] values )
    {
        assertValidSingleNumberPropertyValue( values );
        value = ((Number) values[0]).doubleValue();
        isHighest = false;
    }

    @Override
    public String propertiesAsString()
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    public void initAsLowest()
    {
        value = Double.NEGATIVE_INFINITY;
        isHighest = false;
    }

    @Override
    public void initAsHighest()
    {
        value = Double.POSITIVE_INFINITY;
        isHighest = true;
    }

    @Override
    public String toString()
    {
        return "compareValue=" + value;
    }
}
