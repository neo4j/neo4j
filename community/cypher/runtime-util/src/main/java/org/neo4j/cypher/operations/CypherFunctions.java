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

import org.opencypher.v9_0.util.CypherTypeException;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;

/**
 * This class contains static helper methods for the set of Cypher functions
 */
@SuppressWarnings( "unused" )
public final class CypherFunctions
{
    private CypherFunctions()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static Value sin( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.sin( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "sin()" );
        }
    }

    public static Value asin( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.asin( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "asin()" );
        }
    }

    public static Value haversin( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( (1.0 - Math.cos( ((NumberValue) in).doubleValue() )) / 2 );
        }
        else
        {
            throw needsNumbers( "haversin()" );
        }
    }

    public static Value cos( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.cos( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "cos()" );
        }
    }

    public static Value cot( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( 1.0 / Math.tan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "cot()" );
        }
    }

    public static Value acos( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.acos( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "acos()" );
        }
    }

    public static Value tan( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.tan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "tan()" );
        }
    }

    public static Value atan( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.atan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "atan()" );
        }
    }

    public static Value ceil( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.ceil( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "ceil()" );
        }
    }

    public static Value floor( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.floor( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "floor()" );
        }
    }

    public static Value round( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.round( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "round()" );
        }
    }

    public static Value abs( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            if ( in instanceof IntegralValue )
            {
                return longValue( Math.abs( ((NumberValue) in).longValue() ) );
            }
            else
            {
                return doubleValue( Math.abs( ((NumberValue) in).doubleValue() ) );
            }
        }
        else
        {
            throw needsNumbers( "abs()" );
        }
    }

    public static DoubleValue rand()
    {
        return doubleValue( ThreadLocalRandom.current().nextDouble() );
    }

    private static CypherTypeException needsNumbers( String method )
    {
        return new CypherTypeException( String.format( "%s requires numbers", method ), null );
    }
}
