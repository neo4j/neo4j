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
package org.neo4j.values.storable;

import java.util.Comparator;

/**
 * A tuple of n values.
 */
public class ValueTuple
{
    public static ValueTuple of( Value... values )
    {
        assert values.length > 0 : "Empty ValueTuple is not allowed";
        assert noNulls( values );
        return new ValueTuple( values );
    }

    public static ValueTuple of( Object... objects )
    {
        assert objects.length > 0 : "Empty ValueTuple is not allowed";
        assert noNulls( objects );
        Value[] values = new Value[objects.length];
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = Values.of( objects[i] );
        }
        return new ValueTuple( values );
    }

    private final Value[] values;

    private ValueTuple( Value[] values )
    {
        this.values = values;
    }

    public int size()
    {
        return values.length;
    }

    public Value valueAt( int offset )
    {
        return values[offset];
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ValueTuple that = (ValueTuple) o;

        if ( that.values.length != values.length )
        {
            return false;
        }

        for ( int i = 0; i < values.length; i++ )
        {
            if ( !values[i].equals( that.values[i] ) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = 1;
        for ( Object value : values )
        {
            result = 31 * result + value.hashCode();
        }
        return result;
    }

    public Value getOnlyValue()
    {
        assert values.length == 1 : "Assumed single value tuple, but had " + values.length;
        return values[0];
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        String sep = "( ";
        for ( Value value : values )
        {
            sb.append( sep );
            sep = ", ";
            sb.append( value );
        }
        sb.append( " )" );
        return sb.toString();
    }

    private static boolean noNulls( Object[] values )
    {
        for ( Object v : values )
        {
            if ( v == null )
            {
                return false;
            }
        }
        return true;
    }

    public static final Comparator<ValueTuple> COMPARATOR = ( left, right ) ->
    {
        if ( left.values.length != right.values.length )
        {
            throw new IllegalStateException( "Comparing two ValueTuples of different lengths!" );
        }

        int compare = 0;
        for ( int i = 0; i < left.values.length; i++ )
        {
            compare = Values.COMPARATOR.compare( left.valueAt( i ), right.valueAt( i ) );
            if ( compare != 0 )
            {
                return compare;
            }
        }
        return compare;
    };
}
