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
package org.neo4j.cypher.internal.codegen;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.neo4j.cypher.internal.compiler.v3_3.spi.NodeIdWrapper;
import org.neo4j.cypher.internal.compiler.v3_3.spi.RelationshipIdWrapper;
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException;
import org.neo4j.cypher.internal.frontend.v3_3.IncomparableValuesException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.NodeManager;

import static java.lang.String.format;

// Class with static methods used by compiled execution plans
@SuppressWarnings( "unused" )
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
        else if ( value instanceof Collection<?> )
        {
            return (Collection<?>) value;
        }
        else if ( value instanceof LongStream )
        {
            LongStream stream = (LongStream) value;
            return stream.boxed().collect( Collectors.toList() );
        }
        else if ( value instanceof IntStream )
        {
            IntStream stream = (IntStream) value;
            return stream.boxed().collect( Collectors.toList() );
        }
        else if ( value instanceof DoubleStream )
        {
            DoubleStream stream = (DoubleStream) value;
            return stream.boxed().collect( Collectors.toList() );
        }
        else if (value.getClass().isArray() )
        {
            int len = Array.getLength( value );
            ArrayList<Object> collection = new ArrayList<>( len );
            for ( int i = 0; i < len; i++ )
            {
                collection.add( Array.get( value, i ) );
            }
            return collection;
        }

        throw new CypherTypeException(
                "Don't know how to create an iterable out of " + value.getClass().getSimpleName(), null );
    }

    public static Set<?> toSet( Object value )
    {
        if ( value == null )
        {
            return Collections.emptySet();
        }
        else if ( value instanceof Collection<?> )
        {
            return new HashSet<>( (Collection<?>) value );
        }
        else if ( value instanceof LongStream )
        {
            LongStream stream = (LongStream) value;
            return stream.boxed().collect( Collectors.toSet() );
        }
        else if ( value instanceof IntStream )
        {
            IntStream stream = (IntStream) value;
            return stream.boxed().collect( Collectors.toSet() );
        }
        else if ( value instanceof DoubleStream )
        {
            DoubleStream stream = (DoubleStream) value;
            return stream.boxed().collect( Collectors.toSet() );
        }
        else if (value.getClass().isArray() )
        {
            int len = Array.getLength( value );
            HashSet<Object> collection = new HashSet<>( len );
            for ( int i = 0; i < len; i++ )
            {
                collection.add( Array.get( value, i ) );
            }
            return collection;
        }

        throw new CypherTypeException(
                "Don't know how to create a set out of " + value.getClass().getSimpleName(), null );
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
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

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

        return CompiledEquivalenceUtils.equals( lhs, rhs );
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

    @SuppressWarnings( "unchecked" )
    public static Object loadParameter( Object value )
    {
        if ( value == null )
        {
            return null;
        }
        else if ( value instanceof Node )
        {
            return new NodeIdWrapperImpl( ((Node) value).getId() );
        }
        else if ( value instanceof Relationship )
        {
            return new RelationshipIdWrapperImpl( ((Relationship) value).getId() );
        }
        else if ( value instanceof List<?> )
        {
            List<?> list = (List<?>) value;
            ArrayList<Object> copy = new ArrayList<>( list.size() );
            for ( Object o : list )
            {
                copy.add( loadParameter( o ) );
            }
            return copy;
        }
        else if ( value instanceof Map<?,?> )
        {
            Map<String,?> map = (Map<String,?>) value;
            HashMap<String,Object> copy = new HashMap<>( map.size() );
            for ( Map.Entry<String,?> entry : map.entrySet() )
            {
                copy.put( entry.getKey(), loadParameter( entry.getValue() ) );
            }
            return copy;
        }
        else if ( value.getClass().isArray() )
        {
            Class<?> componentType = value.getClass().getComponentType();
            int length = Array.getLength( value );

            if (componentType.isPrimitive())
            {
                Object copy = Array.newInstance( componentType, length );
                //noinspection SuspiciousSystemArraycopy
                System.arraycopy( value, 0, copy, 0, length );
                return copy;
            }
            else
            {
                Object[] copy = new Object[length];
                for ( int i = 0; i < length; i++ )
                {
                    copy[i] = loadParameter( Array.get(value, i) );
                }
                return copy;
            }
        }
        else
        {
            return value;
        }
    }

    @SuppressWarnings( {"unchecked", "WeakerAccess"} )
    public static Object materializeAnyResult( NodeManager nodeManager, Object anyValue )
    {
        if ( anyValue == null )
        {
            return null;
        }
        else if ( anyValue instanceof NodeIdWrapper )
        {
            return nodeManager.newNodeProxyById( ((NodeIdWrapper) anyValue).id() );
        }
        else if ( anyValue instanceof RelationshipIdWrapper )
        {
            return nodeManager.newRelationshipProxyById( ((RelationshipIdWrapper) anyValue).id() );
        }
        else if ( anyValue instanceof List )
        {
            return ((List) anyValue).stream()
                    .map( v -> materializeAnyResult( nodeManager, v ) ).collect( Collectors.toList() );
        }
        else if ( anyValue instanceof Map )
        {
            Map<String,?> incoming = (Map<String,?>) anyValue;
            HashMap<String,Object> outgoing = new HashMap<>( incoming.size() );
            for ( Map.Entry<String,?> entry : incoming.entrySet() )
            {
                outgoing.put( entry.getKey(), materializeAnyResult( nodeManager, entry.getValue() ) );
            }
            return outgoing;
        }
        else if ( anyValue instanceof PrimitiveNodeStream )
        {
            return ((PrimitiveNodeStream) anyValue).longStream()
                    .mapToObj( nodeManager::newNodeProxyById )
                    .collect( Collectors.toList() );
        }
        else if ( anyValue instanceof PrimitiveRelationshipStream )
        {
            return ((PrimitiveRelationshipStream) anyValue).longStream()
                    .mapToObj( nodeManager::newRelationshipProxyById )
                    .collect( Collectors.toList() );
        }
        else if ( anyValue instanceof LongStream )
        {
            return ((LongStream) anyValue).boxed().collect( Collectors.toList() );
        }
        else if ( anyValue instanceof DoubleStream )
        {
            return ((DoubleStream) anyValue).boxed().collect( Collectors.toList() );
        }
        else if ( anyValue instanceof IntStream )
        {
            // IntStream is only used for list of primitive booleans
            return ((IntStream) anyValue).mapToObj( i -> i != 0 ).collect( Collectors.toList() );
        }
        else if ( anyValue.getClass().isArray())
        {
            Class<?> componentType = anyValue.getClass().getComponentType();
            int length = Array.getLength( anyValue );

            if (componentType.isPrimitive())
            {
                Object copy = Array.newInstance( componentType, length );
                //noinspection SuspiciousSystemArraycopy
                System.arraycopy( anyValue, 0, copy, 0, length );
                return copy;
            }
            else
            {
                Object[] copy = new Object[length];
                for ( int i = 0; i < length; i++ )
                {
                    copy[i] = materializeAnyResult( nodeManager, Array.get(anyValue, i) );
                }
                return copy;
            }
        }
        else
        {
            return anyValue;
        }
    }

    public static Iterator iteratorFrom( Object iterable )
    {
        if ( iterable instanceof Iterable )
        {
            return ((Iterable) iterable).iterator();
        }
        else if ( iterable instanceof PrimitiveEntityStream )
        {
            return ((PrimitiveEntityStream) iterable).iterator();
        }
        else if ( iterable instanceof LongStream )
        {
            return ((LongStream) iterable).iterator();
        }
        else if ( iterable instanceof DoubleStream )
        {
            return ((DoubleStream) iterable).iterator();
        }
        else if ( iterable instanceof IntStream )
        {
            return ((IntStream) iterable).iterator();
        }
        else if ( iterable == null )
        {
            return Collections.emptyIterator();
        }
        else
        {
            throw new CypherTypeException(
                    "Don't know how to create an iterator out of " + iterable.getClass().getSimpleName(), null );
        }
    }

    @SuppressWarnings( "unchecked" )
    public static LongStream toLongStream( Object list )
    {
        if ( list == null )
        {
            return LongStream.empty();
        }
        else if ( list instanceof List )
        {
            return ((List) list).stream().mapToLong( n -> ((Number) n).longValue() );
        }
        else if ( Object[].class.isAssignableFrom( list.getClass() ) )
        {
            return Arrays.stream( (Object[]) list ).mapToLong( n -> ((Number) n).longValue() );
        }
        else if ( list instanceof byte[] )
        {
            byte[] array = (byte[]) list;
            return IntStream.range( 0, array.length ).mapToLong( i -> array[i] );
        }
        else if ( list instanceof short[] )
        {
            short[] array = (short[]) list;
            return IntStream.range( 0, array.length ).mapToLong( i -> array[i] );
        }
        else if ( list instanceof int[] )
        {
            return IntStream.of( (int[]) list ).mapToLong( i -> i );
        }
        else if ( list instanceof long[] )
        {
            return LongStream.of( (long[]) list );
        }
        throw new IllegalArgumentException( format( "Can not be converted to stream: %s", list.getClass().getName() ) );
    }

    @SuppressWarnings( "unchecked" )
    public static DoubleStream toDoubleStream( Object list )
    {
        if ( list == null )
        {
            return DoubleStream.empty();
        }
        else if ( list instanceof List )
        {
            return ((List) list).stream().mapToDouble( n -> ((Number) n).doubleValue() );
        }
        else if ( Object[].class.isAssignableFrom( list.getClass() ) )
        {
            return Arrays.stream( (Object[]) list ).mapToDouble( n -> ((Number) n).doubleValue() );
        }
        else if ( list instanceof float[] )
        {
            float[] array = (float[]) list;
            return IntStream.range( 0, array.length ).mapToDouble( i -> array[i] );
        }
        else if ( list instanceof double[] )
        {
            return DoubleStream.of( (double[]) list );
        }
        throw new IllegalArgumentException( format( "Can not be converted to stream: %s", list.getClass().getName() ) );
    }

    @SuppressWarnings( "unchecked" )
    public static IntStream toBooleanStream( Object list )
    {
        if ( list == null )
        {
            return IntStream.empty();
        }
        else if ( list instanceof List )
        {
            return ((List) list).stream().mapToInt( n -> ((Number) n).intValue() );
        }
        else if ( Object[].class.isAssignableFrom( list.getClass() ) )
        {
            return Arrays.stream( (Object[]) list ).mapToInt( n -> ((Number) n).intValue() );
        }
        else if ( list instanceof boolean[] )
        {
            boolean[] array = (boolean[]) list;
            return IntStream.range( 0, array.length ).map( i -> (array[i]) ? 1 : 0 );
        }
        throw new IllegalArgumentException( format( "Can not be converted to stream: %s", list.getClass().getName() ) );
    }

    @SuppressWarnings( "unused" ) // called from compiled code
    public static long unboxNodeOrNull( NodeIdWrapper value )
    {
        if ( value == null )
        {
            return -1L;
        }
        return value.id();
    }

    @SuppressWarnings( "unused" ) // called from compiled code
    public static long unboxRelationshipOrNull( RelationshipIdWrapper value )
    {
        if ( value == null )
        {
            return -1L;
        }
        return value.id();
    }

    @SuppressWarnings( "unused" ) // called from compiled code
    public static long extractLong( Object obj )
    {
        if ( obj == null )
        {
            return -1L;
        }
        else if ( obj instanceof NodeIdWrapper )
        {
            return ((NodeIdWrapper) obj).id();
        }
        else if ( obj instanceof RelationshipIdWrapper )
        {
            return ((RelationshipIdWrapper) obj).id();
        }
        else if ( obj instanceof Long )
        {
            return (Long) obj;
        }
        else
        {
            throw new IllegalArgumentException(
                    format( "Can not be converted to long: %s", obj.getClass().getName() ) );
        }
    }

}
