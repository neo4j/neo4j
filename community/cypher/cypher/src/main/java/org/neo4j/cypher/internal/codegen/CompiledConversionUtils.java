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
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.values.AnyValue;

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
        else if ( value.getClass().isArray() )
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
        else if ( value.getClass().isArray() )
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

    public static Object loadParameter( AnyValue value, NodeManager manager )
    {
       ParameterConverter converter = new ParameterConverter( manager );
       value.writeTo( converter );
       return converter.value();
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
        else if ( anyValue.getClass().isArray() )
        {
            Class<?> componentType = anyValue.getClass().getComponentType();
            int length = Array.getLength( anyValue );

            if ( componentType.isPrimitive() )
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
                    copy[i] = materializeAnyResult( nodeManager, Array.get( anyValue, i ) );
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

    //In the store we only support primitives, String, and arrays thereof.
    //In cypher we must make an effort to transform Cypher lists to appropriate arrays whenever
    //we are using sending values down to the store or to an index.
    public static Object makeValueNeoSafe( Object object )
    {
        if ( object == null )
        {
            return null;
        }
        if ( hasSafeType( object ) )
        {
            return object;
        }
        else if ( object instanceof Object[] )
        {
            return safeArray( (Object[]) object );
        }
        else if ( object instanceof List<?> )
        {
            return safeArray( (List<?>) object );
        }
        throw new CypherTypeException( "Property values can only be primitive types or arrays thereof", null );
    }

    private static Object safeArray( Object[] array )
    {
        if ( array.length == 0 )
        {
            return new String[0];
        }
        else
        {
            Class<?> type = array[0].getClass();
            for ( int i = 1; i < array.length; i++ )
            {
                type = mergeType( type, array[i].getClass() );
            }

            Object safeArray = Array.newInstance( type, array.length );
            for ( int i = 0; i < array.length; i++ )
            {
                Array.set( safeArray, i, castIt( array[i], type ) );
            }
            return safeArray;
        }
    }

    private static Object safeArray( List<?> list )
    {
        if ( list.size() == 0 )
        {
            return new String[0];
        }
        else
        {
            Class<?> type = null;
            for ( Object o : list )
            {
                if ( type == null )
                {
                    type = o.getClass();
                }
                else
                {
                    type = mergeType( type, o.getClass() );
                }
            }

            Object safeArray = Array.newInstance( type, list.size() );
            int i = 0;
            for ( Object o : list )
            {
                Array.set( safeArray, i++, castIt( o, type ) );
            }
            return safeArray;
        }
    }

    private static Object castIt( Object value, Class<?> type )
    {
        if ( value instanceof Number )
        {
            Number number = (Number) value;
            if ( type == Long.class )
            {
                return number.longValue();
            }
            else if ( type == Integer.class )
            {
                return number.intValue();
            }
            else if ( type == Short.class )
            {
                return number.shortValue();
            }
            else if ( type == Byte.class )
            {
                return number.byteValue();
            }
            else if ( type == Float.class )
            {
                return number.floatValue();
            }
            else if ( type == Double.class )
            {
                return number.doubleValue();
            }
            else
            {
                throw new CypherTypeException( "Cannot handle numbers of type " + type.getName(), null );
            }
        }
        else
        {
            return value;
        }
    }

    private static boolean hasSafeType( Object value )
    {
        if ( value instanceof String )
        {
            return true;
        }
        else if ( value instanceof Long )
        {
            return true;
        }
        else if ( value instanceof Integer )
        {
            return true;
        }
        else if ( value instanceof Boolean )
        {
            return true;
        }
        else if ( value instanceof Double )
        {
            return true;
        }
        else if ( value instanceof Float )
        {
            return true;
        }
        else if ( value instanceof Short )
        {
            return true;
        }
        else if ( value instanceof Byte )
        {
            return true;
        }
        else if ( value.getClass().isArray() && value.getClass().getComponentType().isPrimitive() )
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    static Class<?> mergeType( Class<?> type1, Class<?> type2 )
    {
        if ( type1 == String.class && type2 == String.class )
        {
            return String.class;
        }
        else if ( type1 == Boolean.class && type2 == Boolean.class )
        {
            return Boolean.class;
        }
        else if ( type1 == Character.class && type2 == Character.class )
        {
            return Character.class;
        }
        else if ( Number.class.isAssignableFrom( type1 ) && Number.class.isAssignableFrom( type2 ) )
        {
            if ( type1 == Double.class || type2 == Double.class )
            {
                return Double.class;
            }
            else if ( type1 == Float.class || type2 == Float.class )
            {
                return Float.class;
            }
            else if ( type1 == Long.class || type2 == Long.class )
            {
                return Long.class;
            }
            else if ( type1 == Integer.class || type2 == Integer.class )
            {
                return Integer.class;
            }
            else if ( type1 == Short.class || type2 == Short.class )
            {
                return Short.class;
            }
            else if ( type1 == Byte.class || type2 == Byte.class )
            {
                return Byte.class;
            }
        }
        throw new CypherTypeException( "Property values can only be primitive types or arrays thereof", null );
    }

    @SuppressWarnings( "unchecked" )
    public static Object mapGetProperty( Object object, String key )
    {
        try
        {
            Map<String,Object> map = (Map<String,Object>) object;
            return map.get( key );
        }
        catch ( ClassCastException e )
        {
            throw new CypherTypeException( "Type mismatch: expected a map but was " + object, e );
        }
    }
}
