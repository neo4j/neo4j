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
package org.neo4j.values;

import java.util.Comparator;

import static java.lang.String.format;

// TODO: redo with ValueGroup instead of SemanticType
class ValueComparator implements Comparator<Value>
{
    private final Comparator<Values.SemanticType> semanticTypeComparator;

    ValueComparator( Comparator<Values.SemanticType> semanticTypeComparator )
    {
        this.semanticTypeComparator = semanticTypeComparator;
    }

    @Override
    public int compare( Value o1, Value o2 )
    {
        assert o1 != null && o2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        Values.SemanticType semType1 = semanticType( o1 );
        Values.SemanticType semType2 = semanticType( o2 );

        int x = semanticTypeComparator.compare( semType1, semType2 );

        if ( x == 0 )
        {
            switch ( semType1 )
            {
            case NO_VALUE:
                return x;

            case BOOLEAN:
                return ((DirectBoolean) o1).compareTo( (DirectBoolean) o2 );

            case NUMBER:
                return compareScalarNumbers( o1, o2 );

            case STRING:
                return ((DirectString) o1).compareTo( (DirectString) o2 );

            case BOOLEAN_ARR:
                return ((DirectBooleanArray) o1).compareTo( (DirectBooleanArray) o2 );

            case NUMBER_ARR:
                return compareNumberArrays( o1, o2 );

            case STRING_ARR:
                return ((DirectStringArray) o1).compareTo( (DirectStringArray) o2 );

            default:
                throw new UnsupportedOperationException( format( "Unknown semantic type '%s'", semType1 ) );
            }
        }
        return x;
    }

    private Values.SemanticType semanticType( Value value )
    {
        if ( value instanceof NoValue )
        {
            return Values.SemanticType.NO_VALUE;
        }
        if ( value instanceof DirectString )
        {
            return Values.SemanticType.STRING;
        }
        if ( value instanceof ValueGroup.VNumber )
        {
            return Values.SemanticType.NUMBER;
        }
        if ( value instanceof DirectBoolean )
        {
            return Values.SemanticType.BOOLEAN;
        }
        if ( value instanceof DirectStringArray )
        {
            return Values.SemanticType.STRING_ARR;
        }
        if ( value instanceof DirectIntegralArray || value instanceof DirectFloatingPointArray )
        {
            return Values.SemanticType.NUMBER_ARR;
        }
        if ( value instanceof DirectBooleanArray )
        {
            return Values.SemanticType.BOOLEAN_ARR;
        }

        throw new UnsupportedOperationException(
                format( "Semantic type for value class '%s' is not defined", value.getClass().getName() ) );
    }

    private int compareScalarNumbers( Value o1, Value o2 )
    {
        boolean isInt1 = o1 instanceof DirectIntegralNumber;
        boolean isInt2 = o2 instanceof DirectIntegralNumber;
        if ( isInt1 )
        {
            if ( isInt2 )
            {
                return ((DirectIntegralNumber) o1).compareTo( (DirectIntegralNumber) o2 );
            }
            else
            {
                return ((DirectIntegralNumber) o1).compareTo( (DirectFloatingPointNumber) o2 );
            }
        }
        if ( isInt2 )
        {
            return ((DirectFloatingPointNumber) o1).compareTo( (DirectIntegralNumber) o2 );
        }
        else
        {
            return ((DirectFloatingPointNumber) o1).compareTo( (DirectFloatingPointNumber) o2 );
        }
    }

    private int compareNumberArrays( Value o1, Value o2 )
    {
        boolean isInt1 = o1 instanceof DirectIntegralArray;
        boolean isInt2 = o2 instanceof DirectIntegralArray;
        if ( isInt1 )
        {
            if ( isInt2 )
            {
                return ((DirectIntegralArray) o1).compareTo( (DirectIntegralArray) o2 );
            }
            else
            {
                return ((DirectIntegralArray) o1).compareTo( (DirectFloatingPointArray) o2 );
            }
        }
        if ( isInt2 )
        {
            return ((DirectFloatingPointArray) o1).compareTo( (DirectIntegralArray) o2 );
        }
        else
        {
            return ((DirectFloatingPointArray) o1).compareTo( (DirectFloatingPointArray) o2 );
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj instanceof ValueComparator;
    }
}
