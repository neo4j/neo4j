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
import org.neo4j.values.Values;

import static org.neo4j.kernel.impl.index.schema.NumberValue.DOUBLE;
import static org.neo4j.kernel.impl.index.schema.NumberValue.FLOAT;
import static org.neo4j.kernel.impl.index.schema.NumberValue.LONG;

/**
 * Utilities for converting number values to and from different representations.
 */
class NumberValueConversion
{
    static Number assertValidSingleNumber( Value[] values )
    {
        // TODO: support multiple values, right?
        if ( values.length > 1 )
        {
            throw new IllegalArgumentException( "Tried to create composite key with non-composite schema key layout" );
        }
        if ( values.length < 1 )
        {
            throw new IllegalArgumentException( "Tried to create key without value" );
        }
        if ( !Values.isNumberValue( values[0] ) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support numbers, tried to create key from " + values[0] );
        }
        return (Number) values[0].asObject();
    }

    static Number toValue( byte type, long rawValueBits )
    {
        switch ( type )
        {
        case LONG:
            return rawValueBits;
        case FLOAT:
            return Float.intBitsToFloat( (int)rawValueBits );
        case DOUBLE:
            return Double.longBitsToDouble( rawValueBits );
        default:
            throw new IllegalArgumentException( "Unexpected type " + type );
        }
    }
}
