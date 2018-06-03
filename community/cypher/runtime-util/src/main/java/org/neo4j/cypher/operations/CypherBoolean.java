/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.operations;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * This class contains static helper boolean methods used by the compiled expressions
 */
@SuppressWarnings( "unused" )
public final class CypherBoolean
{
    private CypherBoolean()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static Value or( AnyValue... args )
    {
        boolean seenNull = false;
        for ( AnyValue arg : args )
        {
            if ( arg == NO_VALUE )
            {
                seenNull = true;
                continue;
            }

            if ( arg == Values.TRUE )
            {
                return Values.TRUE;
            }
        }
        return seenNull ? NO_VALUE : Values.FALSE;
    }

    public static Value xor( AnyValue lhs, AnyValue rhs )
    {
        boolean seenNull = false;
        if ( lhs == NO_VALUE || rhs == NO_VALUE )
        {
            return NO_VALUE;
        }

        return (lhs == Values.TRUE) ^ (rhs == Values.TRUE) ? Values.TRUE : Values.FALSE;
    }

    public static Value and( AnyValue... args )
    {
        boolean seenNull = false;
        for ( AnyValue arg : args )
        {
            if ( arg == NO_VALUE )
            {
                seenNull = true;
                continue;
            }

            if ( arg == Values.FALSE )
            {
                return Values.FALSE;
            }
        }
        return seenNull ? NO_VALUE : Values.TRUE;
    }

    public static Value not( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }

        return in != Values.TRUE ? Values.TRUE : Values.FALSE;
    }

    public static Value equals( AnyValue lhs, AnyValue rhs )
    {
        Boolean equals = lhs.ternaryEquals( rhs );
        if ( equals == null )
        {
            return NO_VALUE;
        }
        else
        {
            return equals ? Values.TRUE : Values.FALSE;
        }
    }

    public static Value notEquals( AnyValue lhs, AnyValue rhs )
    {
        Boolean equals = lhs.ternaryEquals( rhs );
        if ( equals == null )
        {
            return NO_VALUE;
        }
        else
        {
            return equals ? Values.FALSE : Values.TRUE;
        }
    }
}
