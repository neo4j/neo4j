/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.function;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Constructors for basic {@link Function} and {@link BiFunction} types
 * @deprecated This class relies on deprecated interfaces, and will be retrofitted to work with the {@code java.util.function} interfaces in 3.0.
 */
public final class Functions
{

    public static <From, To> Function<From,To> withDefaults( final Function<From,To> defaults, final Function<From,
            To> f )
    {
        return from -> {
            To to = f.apply( from );

            if ( to == null )
            {
                return defaults.apply( from );
            }
            else
            {
                return to;
            }
        };
    }

    public static <From, From2, To> BiFunction<? super Function<From,From2>,? super Function<From2,To>,Function<From,To>> compose()
    {
        return ( f1, f2 ) -> (Function<From,To>) from -> f2.apply( f1.apply( from ) );
    }

    public static <T1, T2> BiFunction<? super BiFunction<T1,T2,T1>,? super BiFunction<T1,T2,T1>,BiFunction<T1,T2,T1>> compose2()
    {
        return ( function1, function2 ) ->
                ( from1, from2 ) ->
                        function1.apply( function2.apply( from1, from2 ), from2 );
    }

    public static Function<Object,String> TO_STRING = from -> {
        if ( from != null )
        {
            return from.toString();
        }
        else
        {
            return "";
        }
    };

    public static <T> Function<T,Void> fromConsumer( final Consumer<T> consumer )
    {
        return t -> {
            consumer.accept( t );
            return null;
        };
    }
}
