/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

/**
 * Utility to handle pairs of objects.
 *
 * @param <T1> the type of the {@link #first() first value} of the pair.
 * @param <T2> the type of the {@link #other() other value} of the pair.
 */
public abstract class Pair<T1, T2>
{
    @SuppressWarnings( "rawtypes" )
    private static final Pair EMPTY = Pair.of( null, null );

    @SuppressWarnings( "unchecked" )
    public static <T1, T2> Pair<T1,T2> empty()
    {
        return EMPTY;
    }

    /**
     * Create a new pair of objects.
     *
     * @param first the first object in the pair.
     * @param other the other object in the pair.
     * @param <T1> the type of the first object in the pair
     * @param <T2> the type of the second object in the pair
     * @return a new pair of the two parameters.
     */
    public static <T1, T2> Pair<T1,T2> pair( final T1 first, final T2 other )
    {
        return new Pair<T1,T2>()
        {
            @Override
            public T1 first()
            {
                return first;
            }

            @Override
            public T2 other()
            {
                return other;
            }
        };
    }

    /**
     * Alias of {@link #pair(Object, Object)}.
     * @param first the first object in the pair.
     * @param other the other object in the pair.
     * @param <T1> the type of the first object in the pair
     * @param <T2> the type of the second object in the pair
     * @return a new pair of the two parameters.
     */
    public static <T1, T2> Pair<T1, T2> of( final T1 first, final T2 other )
    {
        return pair( first, other );
    }

    Pair()
    {
        // package private, limited number of subclasses
    }

    /**
     * @return the first object in the pair.
     */
    public abstract T1 first();

    /**
     * @return the other object in the pair.
     */
    public abstract T2 other();

    @Override
    public String toString()
    {
        return "(" + first() + ", " + other() + ")";
    }

    @Override
    public int hashCode()
    {
        return ( 31 * hashCode( first() ) ) | hashCode( other() );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj instanceof Pair )
        {
            @SuppressWarnings( "rawtypes" ) Pair that = (Pair) obj;
            return equals( this.other(), that.other() ) && equals( this.first(), that.first() );
        }
        return false;
    }

    static int hashCode( Object obj )
    {
        return obj == null ? 0 : obj.hashCode();
    }

    static boolean equals( Object obj1, Object obj2 )
    {
        return ( obj1 == obj2 ) || ( obj1 != null && obj1.equals( obj2 ) );
    }
}
