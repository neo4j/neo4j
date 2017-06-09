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
package org.neo4j.values.virtual;

import org.neo4j.values.AnyValue;
import org.neo4j.values.Values;

/**
 * This class is way too similar to org.neo4j.collection.primitive.PrimitiveArrays.
 *
 * Should we introduce dependency on primitive collections?
 */
class ArrayHelpers
{
    static boolean isSortedSet( int[] keys )
    {
        for ( int i = 0; i < keys.length - 1; i++ )
        {
            if ( keys[i] >= keys[i + 1] )
            {
                return false;
            }
        }
        return true;
    }

    static boolean hasNullOrNoValue( AnyValue[] values )
    {
        for ( AnyValue value : values )
        {
            if ( value == null || value == Values.NO_VALUE )
            {
                return true;
            }
        }
        return false;
    }
}
