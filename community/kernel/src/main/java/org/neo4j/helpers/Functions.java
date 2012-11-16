/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.helpers;

import java.util.Map;

public final class Functions
{
    public static <From, To> Function<From, To> map( final Map<From, To> map )
    {
        return new Function<From, To>()
        {
            @Override
            public To apply( From from )
            {
                return map.get( from );
            }
        };
    }

    public static <From, To> Function<From, To> withDefaults( final Function<From, To> defaults, final Function<From,
            To> f )
    {
        return new Function<From, To>()
        {
            @Override
            public To apply( From from )
            {
                To to = f.apply( from );

                if ( to == null )
                {
                    return defaults.apply( from );
                }
                else
                {
                    return to;
                }
            }
        };
    }

    public static <From, To> Function<From, To> nullFunction()
    {
        return new Function<From, To>()
        {
            @Override
            public To apply( From from )
            {
                return null; // Always return null
            }
        };
    }

    public static <T1, T2> Function2<Function2<T1, T2, T1>, Function2<T1, T2, T1>, Function2<T1, T2, T1>> compose()
    {
        return new Function2<Function2<T1, T2, T1>, Function2<T1, T2, T1>, Function2<T1, T2, T1>>()
        {
            @Override
            public Function2<T1, T2, T1> apply( final Function2<T1, T2, T1> function1, final Function2<T1, T2,
                    T1> function2 )
            {
                return new Function2<T1, T2, T1>()
                {
                    @Override
                    public T1 apply( T1 from1, T2 from2 )
                    {
                        return function1.apply( function2.apply( from1, from2 ), from2 );
                    }
                };
            }
        };
    }

    private Functions()
    {
    }
}
