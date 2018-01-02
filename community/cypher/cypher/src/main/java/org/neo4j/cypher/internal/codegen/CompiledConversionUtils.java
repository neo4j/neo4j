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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException;
import org.neo4j.cypher.internal.frontend.v2_3.IncomparableValuesException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.MathUtil;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

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

    public static Boolean equals( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null )
        {
            return null;
        }

        if ( (lhs instanceof NodeIdWrapper && !(rhs instanceof NodeIdWrapper)) ||
             (rhs instanceof NodeIdWrapper && !(lhs instanceof NodeIdWrapper)) ||
             (lhs instanceof RelationshipIdWrapper && !(rhs instanceof RelationshipIdWrapper)) ||
             (rhs instanceof RelationshipIdWrapper && !(lhs instanceof RelationshipIdWrapper)) )
        {

            throw new IncomparableValuesException( lhs.getClass().getSimpleName(), rhs.getClass().getSimpleName() );
        }

        //if floats compare float values if integer types,
        //compare long values
        if ( lhs instanceof Number && rhs instanceof Number )
        {
            if ( (lhs instanceof Double || lhs instanceof Float)
                 && (rhs instanceof Double || rhs instanceof Float) )
            {
                double left = ((Number) lhs).doubleValue();
                double right = ((Number) rhs).doubleValue();
                return left == right;
            }
            else if ( (lhs instanceof Double || lhs instanceof Float) )
            {
                double left = ((Number) lhs).doubleValue();
                long right = ((Number) rhs).longValue();
                return MathUtil.numbersEqual( left, right );
            }
            else if ( (rhs instanceof Double || rhs instanceof Float) )
            {
                long left = ((Number) lhs).longValue();
                double right = ((Number) rhs).doubleValue();
                return MathUtil.numbersEqual( right, left );
            }

            //evertyhing else is long from cyphers point-of-view
            long left = ((Number) lhs).longValue();
            long right = ((Number) rhs).longValue();
            return left == right;
        }

        //for everything else call equals
        return lhs.equals( rhs );
    }

    public static Boolean or( Object lhs, Object rhs )
    {
        if ( lhs == null && rhs == null )
        {
            return null;
        }
        else if ( lhs == null && rhs instanceof Boolean )
        {
            return (Boolean) rhs ? true : null;
        }
        else if ( rhs == null && lhs instanceof Boolean )
        {
            return (Boolean) lhs ? true : null;
        }
        else if ( lhs instanceof Boolean && rhs instanceof Boolean )
        {
            return (Boolean) lhs || (Boolean) rhs;
        }

        throw new CypherTypeException(
                "Don't know how to do or on: " + (lhs != null ? lhs.toString() : null) + " and " +
                (rhs != null ? rhs.toString() : null), null );
    }

    public static Boolean not( Object predicate )
    {
        if ( predicate == null )
        {
            return null;
        }

        if ( predicate instanceof Boolean )
        {
            return !(Boolean) predicate;
        }

        throw new CypherTypeException( "Don't know how to treat that as a boolean: " + predicate.toString(), null );
    }

    public static Object loadParameter( Object value )
    {
        if ( value instanceof Node )
        {
            return new NodeIdWrapper( ((Node) value).getId() );
        }
        else if ( value instanceof Relationship )
        {
            return new RelationshipIdWrapper( ((Relationship) value).getId() );
        }
        else
        {
            return value;
        }
    }
}
