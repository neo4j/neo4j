/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.annotations.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Should contain all the defferent types
 */
@PublicApi
public class PublicClass<T, V extends Serializable & Comparable<? super PublicClass>>
        extends ArrayList<T>
        implements Serializable, Cloneable, Comparable<PublicClass<T,V>>
{
    public static final long NUMBER = 42;
    public final String name = "constant";
    public T value;

    public String stringMethod()
    {
        return "";
    }

    /**
     * Should not be included
     */
    private static void dummy1()
    {
    }
    static void dummy2()
    {
    }

    public void manyArguments( boolean b, List<String> l, Supplier<? extends PublicClass<T,V>> a ) throws IOException
    {
    }

    @Override
    public int compareTo( PublicClass<T,V> o )
    {
        return 0;
    }

    public class InnerClass
    {
        public InnerClass( int i )
        {
        }

        public String test()
        {
            return "";
        }
    }

    public interface InnerInterface extends Serializable
    {
        long test( int a );
        void varArgs1( int[] a);
        void varArgs2( int... a);
        void varArgs3( int[]... a);

        default <V> int defaultMethod( V v )
        {
            return 0;
        }
    }

    public enum InnerEnum
    {
        E1,
        E2;
    }
}
