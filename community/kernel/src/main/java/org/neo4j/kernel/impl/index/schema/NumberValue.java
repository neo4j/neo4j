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
import org.neo4j.values.Value;

import static org.neo4j.kernel.impl.index.schema.NumberValueConversion.toValue;

/**
 * Value in a {@link GBPTree} handling numbers suitable for schema indexing.
 * Contains actual number for internal filtering after accidental query hits due to double value coersion.
 */
class NumberValue
{
    static final int SIZE =
            Byte.SIZE + /* type */
            Long.SIZE;  /* value bits */

    static final byte LONG = 0;
    static final byte FLOAT = 1;
    static final byte DOUBLE = 2;

    byte type;
    long rawValueBits;

    void from( Value[] values )
    {
        extractValue( NumberValueConversion.assertValidSingleNumber( values ) );
    }

    byte type()
    {
        return type;
    }

    long rawValueBits()
    {
        return rawValueBits;
    }

    private void extractValue( Number value )
    {
        if ( value instanceof Double )
        {
            type = DOUBLE;
            rawValueBits = Double.doubleToLongBits( (Double) value );
        }
        else if ( value instanceof Float )
        {
            type = FLOAT;
            rawValueBits = Float.floatToIntBits( (Float) value );
        }
        else
        {
            type = LONG;
            rawValueBits = value.longValue();
        }
    }

    @Override
    public String toString()
    {
        return "type=" + type + ",rawValue=" + rawValueBits + ",value=" + toValue( type, rawValueBits );
    }
}
