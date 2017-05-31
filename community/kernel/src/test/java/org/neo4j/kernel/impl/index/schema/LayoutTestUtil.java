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

import org.neo4j.index.internal.gbptree.Layout;

import static org.neo4j.kernel.impl.index.schema.NumberValue.DOUBLE;
import static org.neo4j.kernel.impl.index.schema.NumberValue.FLOAT;
import static org.neo4j.kernel.impl.index.schema.NumberValue.LONG;

abstract class LayoutTestUtil<KEY extends NumberKey, VALUE extends NumberValue>
{
    abstract Layout<KEY,VALUE> createLayout();

    abstract void copyValue( VALUE value, VALUE intoValue );

    int compareValue( VALUE value1, VALUE value2 )
    {
        return compareIndexedPropertyValue( value1, value2 );
    }

    int compareIndexedPropertyValue( NumberValue value1, NumberValue value2 )
    {
        int typeCompare = Byte.compare( value1.type(), value2.type() );
        if ( typeCompare == 0 )
        {
            switch ( value1.type() )
            {
            case LONG:
                return Long.compare( value1.rawValueBits(), value2.rawValueBits() );
            case FLOAT:
                return Float.compare(
                        Float.intBitsToFloat( (int) value1.rawValueBits() ),
                        Float.intBitsToFloat( (int) value2.rawValueBits() ) );
            case DOUBLE:
                return Double.compare(
                        Double.longBitsToDouble( value1.rawValueBits() ),
                        Double.longBitsToDouble( value2.rawValueBits() ) );
            default:
                throw new IllegalArgumentException(
                        "Expected type to be LONG, FLOAT or DOUBLE (" + LONG + "," + FLOAT + "," + DOUBLE +
                                "). But was " + value1.type() );
            }
        }
        return typeCompare;
    }
}
