/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.cypher.internal.compiler.v2_3.CypherTypeException;
import org.neo4j.helpers.collection.MapUtil;

// Class with static methods used by compiled execution plans
public abstract class CompiledConversionUtils
{
    public static boolean coerceToPredicate( Object value )
    {
        if ( value == null )
        {
            return false;
        }
        if ( value instanceof Boolean )
        {
            return (boolean) value;
        }
        if ( value.getClass().isArray() )
        {
            return Array.getLength( value ) > 0;
        }
        throw new CypherTypeException( "Don't know how to treat that as a predicate: " + value.toString(), null );
    }

    public static Collection<?> toCollection( Object value )
    {
        if ( value == null )
        {
            return Collections.emptyList();
        }
        if ( value instanceof Collection<?> )
        {
            return ((Collection<?>) value);
        }

        throw new CypherTypeException( "Don't know how to create an iterable out of " + value.getClass().getSimpleName(), null );
    }

    public static CompositeKey compositeKey( long... keys )
    {
        return new CompositeKey( keys );
    }

    public static class CompositeKey
    {
        private final long[] key;

        private CompositeKey( long[] key )
        {
            this.key = key;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            CompositeKey that = (CompositeKey) o;

            return Arrays.equals( key, that.key );

        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( key );
        }
    }
}
