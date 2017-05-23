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
                return ((BooleanValue) o1).compareTo( (BooleanValue) o2 );

            case NUMBER:
                return compareScalarNumbers( o1, o2 );

            case STRING:
                return ((StringValue) o1).compareTo( (StringValue) o2 );

            case BOOLEAN_ARR:
                return ((BooleanArrayValue) o1).compareTo( (BooleanArrayValue) o2 );

            case NUMBER_ARR:
                return compareNumberArrays( o1, o2 );

            case STRING_ARR:
                return ((StringArrayValue) o1).compareTo( (StringArrayValue) o2 );

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
        if ( value instanceof StringValue )
        {
            return Values.SemanticType.STRING;
        }
        if ( value instanceof NumberValue )
        {
            return Values.SemanticType.NUMBER;
        }
        if ( value instanceof BooleanValue )
        {
            return Values.SemanticType.BOOLEAN;
        }
        if ( value instanceof StringArrayValue )
        {
            return Values.SemanticType.STRING_ARR;
        }
        if ( value instanceof IntegralArrayValue || value instanceof FloatingPointArrayValue )
        {
            return Values.SemanticType.NUMBER_ARR;
        }
        if ( value instanceof BooleanArrayValue )
        {
            return Values.SemanticType.BOOLEAN_ARR;
        }

        throw new UnsupportedOperationException(
                format( "Semantic type for value class '%s' is not defined", value.getClass().getName() ) );
    }

    private int compareScalarNumbers( Value o1, Value o2 )
    {
        boolean isInt1 = o1 instanceof IntegralNumberValue;
        boolean isInt2 = o2 instanceof IntegralNumberValue;
        if ( isInt1 )
        {
            if ( isInt2 )
            {
                return ((IntegralNumberValue) o1).compareTo( (IntegralNumberValue) o2 );
            }
            else
            {
                return ((IntegralNumberValue) o1).compareTo( (FloatingPointNumberValue) o2 );
            }
        }
        if ( isInt2 )
        {
            return ((FloatingPointNumberValue) o1).compareTo( (IntegralNumberValue) o2 );
        }
        else
        {
            return ((FloatingPointNumberValue) o1).compareTo( (FloatingPointNumberValue) o2 );
        }
    }

    private int compareNumberArrays( Value o1, Value o2 )
    {
        boolean isInt1 = o1 instanceof IntegralArrayValue;
        boolean isInt2 = o2 instanceof IntegralArrayValue;
        if ( isInt1 )
        {
            if ( isInt2 )
            {
                return ((IntegralArrayValue) o1).compareTo( (IntegralArrayValue) o2 );
            }
            else
            {
                return ((IntegralArrayValue) o1).compareTo( (FloatingPointArrayValue) o2 );
            }
        }
        if ( isInt2 )
        {
            return ((FloatingPointArrayValue) o1).compareTo( (IntegralArrayValue) o2 );
        }
        else
        {
            return ((FloatingPointArrayValue) o1).compareTo( (FloatingPointArrayValue) o2 );
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj instanceof ValueComparator;
    }
}
