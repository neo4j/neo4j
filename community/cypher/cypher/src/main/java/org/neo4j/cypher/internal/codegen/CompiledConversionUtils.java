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

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.neo4j.cypher.internal.compiler.v3_4.spi.NodeIdWrapper;
import org.neo4j.cypher.internal.compiler.v3_4.spi.RelationshipIdWrapper;
import org.neo4j.cypher.internal.util.v3_4.CypherTypeException;
import org.neo4j.cypher.internal.util.v3_4.IncomparableValuesException;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.String.format;
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.storable.Values.NO_VALUE;

// Class with static methods used by compiled execution plans
@SuppressWarnings( "unused" )
public abstract class CompiledConversionUtils
{
    public static boolean coerceToPredicate( Object value )
    {
        if ( value == null || value == Values.NO_VALUE )
        {
            return false;
        }
        if ( value instanceof BooleanValue )
        {
            return ((BooleanValue) value).booleanValue();
        }
        if ( value instanceof Boolean )
        {
            return (boolean) value;
        }
        if ( value instanceof ArrayValue )
        {
            return ((ArrayValue) value).length() > 0;
        }
        if ( value.getClass().isArray() )
        {
            return Array.getLength( value ) > 0;
        }
        throw new CypherTypeException( "Don't know how to treat that as a predicate: " + value.toString(), null );
    }

//    public static Collection<?> toCollection( Object value )
//    {
//        if ( value == null )
//        {
//            return Collections.emptyList();
//        }
//        else if ( value instanceof SequenceValue )
//        {
//            // TODO: FIXME
//        }
//        else if ( value instanceof Collection<?> )
//        {
//            return (Collection<?>) value;
//        }
//        else if ( value instanceof LongStream )
//        {
//            LongStream stream = (LongStream) value;
//            return stream.boxed().collect( Collectors.toList() );
//        }
//        else if ( value instanceof IntStream )
//        {
//            IntStream stream = (IntStream) value;
//            return stream.boxed().collect( Collectors.toList() );
//        }
//        else if ( value instanceof DoubleStream )
//        {
//            DoubleStream stream = (DoubleStream) value;
//            return stream.boxed().collect( Collectors.toList() );
//        }
//        else if ( value.getClass().isArray() )
//        {
//            int len = Array.getLength( value );
//            ArrayList<Object> collection = new ArrayList<>( len );
//            for ( int i = 0; i < len; i++ )
//            {
//                collection.add( Array.get( value, i ) );
//            }
//            return collection;
//        }
//
//        throw new CypherTypeException(
//                "Don't know how to create an iterable out of " + value.getClass().getSimpleName(), null );
//    }

    public static Set<?> toSet( Object value )
    {
        if ( value == null || value instanceof NoValue )
        {
            return Collections.emptySet();
        }
        else if ( value instanceof SequenceValue )
        {
            SequenceValue sequenceValue = (SequenceValue) value;
            Iterator<AnyValue> iterator = sequenceValue.iterator();
            Set<AnyValue> set = sequenceValue.iterationPreference() == RANDOM_ACCESS ?
                                // If we have a random access sequence value length() should be cheap and we can optimize the initial capacity
                                new HashSet<>( sequenceValue.length() ) : new HashSet<>();

            while ( iterator.hasNext() )
            {
                AnyValue element = iterator.next();
                set.add( element );
            }
            return set;
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

    /**
     * Checks equality according to OpenCypher
     * @return true if equal, false if not equal and null if incomparable
     */
    public static Boolean equals( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null )
        {
            return null;
        }

//        boolean lhsNodeIdWrapper = lhs instanceof NodeIdWrapper;
//        if ( lhsNodeIdWrapper || rhs instanceof NodeIdWrapper || lhs instanceof RelationshipIdWrapper ||
//             rhs instanceof RelationshipIdWrapper )
//        {
//            if ( (lhsNodeIdWrapper && !(rhs instanceof NodeIdWrapper)) ||
//                 (rhs instanceof NodeIdWrapper && !lhsNodeIdWrapper) ||
//                 (lhs instanceof RelationshipIdWrapper && !(rhs instanceof RelationshipIdWrapper)) ||
//                 (rhs instanceof RelationshipIdWrapper && !(lhs instanceof RelationshipIdWrapper)) )
//            {
//                throw new IncomparableValuesException( lhs.getClass().getSimpleName(), rhs.getClass().getSimpleName() );
//            }
//            return lhs.equals( rhs );
//        }

        boolean lhsVirtualNodeValue = lhs instanceof VirtualNodeValue;
        if ( lhsVirtualNodeValue || rhs instanceof VirtualNodeValue || lhs instanceof VirtualRelationshipValue ||
             rhs instanceof VirtualRelationshipValue )
        {
            if ( (lhsVirtualNodeValue && !(rhs instanceof VirtualNodeValue)) ||
                 (rhs instanceof VirtualNodeValue && !lhsVirtualNodeValue) ||
                 (lhs instanceof VirtualRelationshipValue && !(rhs instanceof VirtualRelationshipValue)) ||
                 (rhs instanceof VirtualRelationshipValue && !(lhs instanceof VirtualRelationshipValue)) )
            {
                throw new IncomparableValuesException( lhs.getClass().getSimpleName(), rhs.getClass().getSimpleName() );
            }
            return lhs.equals( rhs );
        }

        AnyValue lhsValue = lhs instanceof AnyValue ? (AnyValue) lhs : ValueUtils.of( lhs );
        AnyValue rhsValue = rhs instanceof AnyValue ? (AnyValue) rhs : ValueUtils.of( rhs );

        return lhsValue.ternaryEquals( rhsValue );
    }

    public static Boolean or( Object lhs, Object rhs )
    {
        // TODO: Handle BooleanValue and NoValue
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
        // TODO: Handle BooleanValue and NoValue
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

    public static Object loadParameter( AnyValue value, EmbeddedProxySPI proxySpi )
    {
//       ParameterConverter converter = new ParameterConverter( proxySpi );
//       value.writeTo( converter );
//       return converter.value();
        return value;
    }

    @SuppressWarnings( {"unchecked", "WeakerAccess"} )
//    public static Object materializeAnyResult( EmbeddedProxySPI proxySpi, Object anyValue )
//    {
//        if ( anyValue == null )
//        {
//            return null;
//        }
//        else if ( anyValue instanceof VirtualNodeValue )
//        {
//            if ( anyValue instanceof NodeValue )
//            {
//                return anyValue;
//            }
//            return proxySpi.newNodeProxy( ((NodeIdWrapper) anyValue).id() );
//        }
//        else if ( anyValue instanceof VirtualRelationshipValue )
//        {
//            if ( anyValue instanceof RelationshipValue )
//            {
//                return anyValue;
//            }
//            return proxySpi.newRelationshipProxy( ((RelationshipIdWrapper) anyValue).id() );
//        }
//        else if ( anyValue instanceof List )
//        {
//            return ((List) anyValue).stream()
//                    .map( v -> materializeAnyResult( proxySpi, v ) ).collect( Collectors.toList() );
//        }
//        else if ( anyValue instanceof Map )
//        {
//            Map<String,?> incoming = (Map<String,?>) anyValue;
//            HashMap<String,Object> outgoing = new HashMap<>( incoming.size() );
//            for ( Map.Entry<String,?> entry : incoming.entrySet() )
//            {
//                outgoing.put( entry.getKey(), materializeAnyResult( proxySpi, entry.getValue() ) );
//            }
//            return outgoing;
//        }
//        else if ( anyValue instanceof PrimitiveNodeStream )
//        {
//            return ((PrimitiveNodeStream) anyValue).longStream()
//                    .mapToObj( proxySpi::newNodeProxy )
//                    .collect( Collectors.toList() );
//        }
//        else if ( anyValue instanceof PrimitiveRelationshipStream )
//        {
//            return ((PrimitiveRelationshipStream) anyValue).longStream()
//                    .mapToObj( proxySpi::newRelationshipProxy )
//                    .collect( Collectors.toList() );
//        }
//        else if ( anyValue instanceof LongStream )
//        {
//            return ((LongStream) anyValue).boxed().collect( Collectors.toList() );
//        }
//        else if ( anyValue instanceof DoubleStream )
//        {
//            return ((DoubleStream) anyValue).boxed().collect( Collectors.toList() );
//        }
//        else if ( anyValue instanceof IntStream )
//        {
//            // IntStream is only used for list of primitive booleans
//            return ((IntStream) anyValue).mapToObj( i -> i != 0 ).collect( Collectors.toList() );
//        }
//        else if ( anyValue.getClass().isArray() )
//        {
//            Class<?> componentType = anyValue.getClass().getComponentType();
//            int length = Array.getLength( anyValue );
//
//            if ( componentType.isPrimitive() )
//            {
//                Object copy = Array.newInstance( componentType, length );
//                //noinspection SuspiciousSystemArraycopy
//                System.arraycopy( anyValue, 0, copy, 0, length );
//                return copy;
//            }
//            else if ( anyValue instanceof String[] )
//            {
//                return anyValue;
//            }
//            else
//            {
//                Object[] copy = new Object[length];
//                for ( int i = 0; i < length; i++ )
//                {
//                    copy[i] = materializeAnyResult( proxySpi, Array.get( anyValue, i ) );
//                }
//                return copy;
//            }
//        }
//        // TODO: Do we need to do anything for Values (sequence or map) ?
//        else
//        {
//            return anyValue;
//        }
//    }

    public static AnyValue materializeAnyResult( EmbeddedProxySPI proxySpi, Object anyValue )
    {
        if ( anyValue == null || anyValue == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( anyValue instanceof VirtualNodeValue )
        {
            if ( anyValue instanceof NodeValue )
            {
                return (AnyValue) anyValue;
            }
            return ValueUtils.fromNodeProxy( proxySpi.newNodeProxy( ((VirtualNodeValue) anyValue).id() ) );
        }
        else if ( anyValue instanceof VirtualRelationshipValue )
        {
            if ( anyValue instanceof RelationshipValue )
            {
                return (AnyValue) anyValue;
            }
            return ValueUtils.fromRelationshipProxy( proxySpi.newRelationshipProxy( ((VirtualRelationshipValue) anyValue).id() ) );
        }
        else if ( anyValue instanceof List )
        {
            return VirtualValues.fromList( (List<AnyValue>) (((List) anyValue).stream()
                    .map( v -> materializeAnyResult( proxySpi, v ) ).collect( Collectors.toList() ) ) );
        }
        else if ( anyValue instanceof Map )
        {
            Map<String,?> incoming = (Map<String,?>) anyValue;
            HashMap<String,AnyValue> outgoing = new HashMap<>( incoming.size() );
            for ( Map.Entry<String,?> entry : incoming.entrySet() )
            {
                outgoing.put( entry.getKey(), materializeAnyResult( proxySpi, entry.getValue() ) );
            }
            return VirtualValues.map( outgoing );
        }
        else if ( anyValue instanceof PrimitiveNodeStream )
        {
            return VirtualValues.fromList( ((PrimitiveNodeStream) anyValue).longStream()
                    .mapToObj( id -> (AnyValue) ValueUtils.fromNodeProxy( proxySpi.newNodeProxy( id ) ) )
                    .collect( Collectors.toList() ) );
        }
        else if ( anyValue instanceof PrimitiveRelationshipStream )
        {
            return VirtualValues.fromList( ((PrimitiveRelationshipStream) anyValue).longStream()
                    .mapToObj( id -> (AnyValue) ValueUtils.fromRelationshipProxy( proxySpi.newRelationshipProxy( id ) ) )
                    .collect( Collectors.toList() ) );
        }
        else if ( anyValue instanceof LongStream )
        {
            long[] array = ((LongStream) anyValue).toArray();
            return Values.longArray( array );
        }
        else if ( anyValue instanceof DoubleStream )
        {
            double[] array = ((DoubleStream) anyValue).toArray();
            return Values.doubleArray( array );
        }
        else if ( anyValue instanceof IntStream )
        {
            // IntStream is only used for list of primitive booleans
            return VirtualValues.fromList( ((IntStream) anyValue).mapToObj( i -> Values.booleanValue( i != 0 ) ).collect( Collectors.toList() ) );
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
                return ValueUtils.of( copy );
            }
            else if ( anyValue instanceof String[] )
            {
                return Values.stringArray( (String[]) anyValue );
            }
            else
            {
                AnyValue[] copy = new AnyValue[length];
                for ( int i = 0; i < length; i++ )
                {
                    copy[i] = materializeAnyResult( proxySpi, Array.get( anyValue, i ) );
                }
                return ValueUtils.of( copy );
            }
        }
        // TODO: Do we need to do anything for Values (sequence or map) ?
        // Yes, run through a modified DefaultValueMapper, without the mappings to Java types, that will create proxy objects for entities
        else if ( anyValue instanceof AnyValue )
        {
            return (AnyValue) anyValue;
        }
        else
        {
            return ValueUtils.of( anyValue );
        }
//        else
//        {
//            throw new IllegalArgumentException( "Do not know how to materialize value of type " + anyValue.getClass().getName() );
//        }
    }

    public static NodeValue materializeNodeValue( EmbeddedProxySPI proxySpi, Object anyValue )
    {
        // Null check has to be done outside by the generated code
        if ( anyValue instanceof NodeValue )
        {
            return (NodeValue) anyValue;
        }
        else if ( anyValue instanceof VirtualNodeValue )
        {
            return ValueUtils.fromNodeProxy( proxySpi.newNodeProxy( ((VirtualNodeValue) anyValue).id() ) );
        }
        throw new IllegalArgumentException( "Do not know how to materialize node value from type " + anyValue.getClass().getName() );
    }

    public static RelationshipValue materializeRelationshipValue( EmbeddedProxySPI proxySpi, Object anyValue )
    {
        // Null check has to be done outside by the generated code
        if ( anyValue instanceof RelationshipValue )
        {
            return (RelationshipValue) anyValue;
        }
        else if ( anyValue instanceof VirtualRelationshipValue )
        {
            return ValueUtils.fromRelationshipProxy( proxySpi.newRelationshipProxy( ((VirtualRelationshipValue) anyValue).id() ) );
        }
        throw new IllegalArgumentException( "Do not know how to materialize relationship value from type " + anyValue.getClass().getName() );
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
        else if ( iterable.getClass().isArray() )
        {
            return new ArrayIterator( iterable );
        }
        else
        {
            return Stream.of(iterable).iterator();
        }
    }

    @SuppressWarnings( "unchecked" )
    public static LongStream toLongStream( Object list )
    {
        if ( list == null )
        {
            return LongStream.empty();
        }
        else if ( list instanceof SequenceValue )
        {
            throw new IllegalArgumentException( "Need to implement support for SequenceValue in CompiledConversionUtils.toLongStream" );
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
        else if ( list instanceof SequenceValue )
        {
            throw new IllegalArgumentException( "Need to implement support for SequenceValue in CompiledConversionUtils.toDoubleStream" );
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
        else if ( list instanceof SequenceValue )
        {
            throw new IllegalArgumentException( "Need to implement support for SequenceValue in CompiledConversionUtils.toBooleanStream" );
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
        else if ( obj instanceof VirtualNodeValue )
        {
            return ((VirtualNodeValue) obj).id();
        }
        else if ( obj instanceof VirtualRelationshipValue )
        {
            return ((VirtualRelationshipValue) obj).id();
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

    public static Object makeValueNeoSafe( Object object )
    {
        AnyValue value = object instanceof AnyValue ? ((AnyValue) object) : ValueUtils.of( object );
        return org.neo4j.cypher.internal.runtime.interpreted.makeValueNeoSafe.apply( value );
    }

    //In the store we only support primitives, String, and arrays thereof.
    //In cypher we must make an effort to transform Cypher lists to appropriate arrays whenever
    //we are using sending values down to the store or to an index.
    public static Object makeValueNeoSafeDeprecated( Object object )
//    public static Object makeValueNeoSafe( Object object )
    {
        if ( object == null )
        {
            return null;
        }
        if ( hasSafeType( object ) )
        {
            return object;
        }
        else if ( object instanceof ListValue )
        {
            ListValue listValue = (ListValue) object;
            if ( listValue.storable() )
            {
                return listValue.toStorableArray();
            }
            else if ( listValue.isEmpty() )
            {
                return Values.stringArray();
            }
            else
            {
                if ( listValue.iterationPreference() == RANDOM_ACCESS )
                {
                    // TODO: Wrap in an ArrayValue based on the type
                    return safeArray( listValue.asArray() );
                }
                else
                {
                    // TODO: Wrap in an ArrayValue based on the type
                    return safeArray( listValue );
                }
            }
        }
        // TODO: Handle ArrayValue
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

    // TODO: Improvement: If we only ever have the same type in an ArrayValue we do not need create a new array!

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
                // TODO: Check conditions of PointValue
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
            Object value = null;
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
                // TODO: Check conditions of PointValue
                Array.set( safeArray, i++, castIt( o, type ) );
            }
            return safeArray;
        }
    }

    private static Object safeArray( Iterable<AnyValue> iterable )
    {
        Iterator<AnyValue> typeIterator = iterable.iterator();

        if ( !typeIterator.hasNext() )
        {
            return new String[0];
        }
        else
        {
            // Determine common value type
            AnyValue value = typeIterator.next();
            Class<?> type = value.getClass();
            int length = 1;
            while ( typeIterator.hasNext() )
            {
                length++;
                value = typeIterator.next();
                type = mergeType( type, value.getClass() );
            }

            // Create a new array of the merged type
            Object safeArray = Array.newInstance( type, length );

            // Fill it with values
            Iterator<AnyValue> valueIterator = iterable.iterator();
            int i = 0;
            while ( valueIterator.hasNext() )
            {
                AnyValue value2 = valueIterator.next();
                Array.set( safeArray, i++, castIt( value2, type ) );
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
        return value instanceof Value ||
                value instanceof String ||
                value instanceof Long ||
                value instanceof Integer ||
                value instanceof Boolean ||
                value instanceof Double ||
                value instanceof Float ||
                value instanceof Short ||
                value instanceof Byte ||
                (value.getClass().isArray() && value.getClass().getComponentType().isPrimitive());
    }

    static Class<?> mergeType( Class<?> type1, Class<?> type2 )
    {
        if ( Value.class.isAssignableFrom( type1 ) &&  Value.class.isAssignableFrom( type2 ) )
        {
            if ( type1 == type2 && ( type1 == TextValue.class ||
                                     type1 == BooleanValue.class ||
                                     type1 == PointValue.class ||
                                     type1 == Duration.class ||
                                     TemporalValue.class.isAssignableFrom( type1 ) ) )
            {
                return type1;
            }
            else if ( NumberValue.class.isAssignableFrom( type1 ) && NumberValue.class.isAssignableFrom( type2 ) )
            {
                if ( type1 == DoubleValue.class || type2 == DoubleValue.class )
                {
                    return DoubleValue.class;
                }
                else if ( type1 == FloatValue.class || type2 == FloatValue.class )
                {
                    return FloatValue.class;
                }
                else if ( type1 == LongValue.class || type2 == LongValue.class )
                {
                    return LongValue.class;
                }
                else if ( type1 == IntValue.class || type2 == IntValue.class )
                {
                    return IntValue.class;
                }
                else if ( type1 == ShortValue.class || type2 == ShortValue.class )
                {
                    return ShortValue.class;
                }
                else if ( type1 == ByteValue.class || type2 == ByteValue.class )
                {
                    return ByteValue.class;
                }
            }
            else if ( type1 == NoValue.class || type2 == NoValue.class )
            {
                throw new CypherTypeException( "Collections containing null values can not be stored in properties.", null );
            }
            else if ( ListValue.class.isAssignableFrom( type1 ) || ListValue.class.isAssignableFrom( type2 ) )
            {
                throw new CypherTypeException( "Collections containing collections can not be stored in properties.", null );
            }
/*
      case (p1: PointValue, p2: PointValue) =>
        if (p1.getCoordinateReferenceSystem != p2.getCoordinateReferenceSystem) {
          throw new CypherTypeException("Collections containing point values with different CRS can not be stored in properties.");
        } else if(p1.coordinate().length != p2.coordinate().length) {
          throw new CypherTypeException("Collections containing point values with different dimensions can not be stored in properties.");
        } else {
          p1
        }
 */
        }
        else if ( type1 == String.class && type2 == String.class )
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
        else if ( type1 == type2 && ( Temporal.class.isAssignableFrom( type1 ) ||
                                      TemporalAmount.class.isAssignableFrom( type1 ) ||
                                      Point.class.isAssignableFrom( type1 ) ) )
        {
            return type1;
        }
        throw new CypherTypeException( "Property values can only be primitive types or arrays thereof.", null );
    }

    private static ArrayValue makeArrayValue( Object array, Class<?> type )
    {
        switch ( type.getName() )
        {
        case "org.neo4j.values.storable.CharValue":
            break;
        case "org.neo4j.values.storable.TextValue":
            break;
        case "org.neo4j.values.storable.BooleanValue":
            break;
        case "org.neo4j.values.storable.ByteValue":
            break;
        case "org.neo4j.values.storable.ShortValue":
            break;
        case "org.neo4j.values.storable.IntValue":
            break;
        case "org.neo4j.values.storable.LongValue":
            break;
        case "org.neo4j.values.storable.FloatValue":
            break;
        case "org.neo4j.values.storable.DoubleValue":
            break;
        case "org.neo4j.values.storable.PointValue":
            break;
        case "org.neo4j.values.storable.Point": // Really?
            break;
        case "org.neo4j.values.storable.DurationValue":
            break;
        case "org.neo4j.values.storable.DateTimeValue":
            break;
        case "org.neo4j.values.storable.DateValue":
            break;
        case "org.neo4j.values.storable.LocalTimeValue":
            break;
        case "org.neo4j.values.storable.TimeValue":
            break;
        case "org.neo4j.values.storable.LocalDateTimeValue":
            break;
        default:
            throw new CypherTypeException( "Property values can only be of primitive types or arrays thereof", null );
        }
        return null;
    }

    /*
      def getConverter(x: AnyValue): Converter = x match {
    case _: CharValue => Converter(
      transform(new ArrayConverterWriter(classOf[Char], a => Values.charArray(a.asInstanceOf[Array[Char]]))))
    case _: TextValue => Converter(
      transform(new ArrayConverterWriter(classOf[String], a => Values.stringArray(a.asInstanceOf[Array[String]]: _*))))
    case _: BooleanValue => Converter(
      transform(new ArrayConverterWriter(classOf[Boolean], a => Values.booleanArray(a.asInstanceOf[Array[Boolean]]))))
    case _: ByteValue => Converter(
      transform(new ArrayConverterWriter(classOf[Byte], a => Values.byteArray(a.asInstanceOf[Array[Byte]]))))
    case _: ShortValue => Converter(
      transform(new ArrayConverterWriter(classOf[Short], a => Values.shortArray(a.asInstanceOf[Array[Short]]))))
    case _: IntValue => Converter(
      transform(new ArrayConverterWriter(classOf[Int], a => Values.intArray(a.asInstanceOf[Array[Int]]))))
    case _: LongValue => Converter(
      transform(new ArrayConverterWriter(classOf[Long], a => Values.longArray(a.asInstanceOf[Array[Long]]))))
    case _: FloatValue => Converter(
      transform(new ArrayConverterWriter(classOf[Float], a => Values.floatArray(a.asInstanceOf[Array[Float]]))))
    case _: DoubleValue => Converter(
      transform(new ArrayConverterWriter(classOf[Double], a => Values.doubleArray(a.asInstanceOf[Array[Double]]))))
    case _: PointValue => Converter(
      transform(new ArrayConverterWriter(classOf[PointValue], a => Values.pointArray(a.asInstanceOf[Array[PointValue]]))))
    case _: Point => Converter(
      transform(new ArrayConverterWriter(classOf[Point], a => Values.pointArray(a.asInstanceOf[Array[Point]]))))
    case _: DurationValue => Converter(
      transform(new ArrayConverterWriter(classOf[TemporalAmount], a => Values.durationArray(a.asInstanceOf[Array[TemporalAmount]]))))
    case _: DateTimeValue => Converter(
      transform(new ArrayConverterWriter(classOf[ZonedDateTime], a => Values.dateTimeArray(a.asInstanceOf[Array[ZonedDateTime]]))))
    case _: DateValue => Converter(
      transform(new ArrayConverterWriter(classOf[LocalDate], a => Values.dateArray(a.asInstanceOf[Array[LocalDate]]))))
    case _: LocalTimeValue => Converter(
      transform(new ArrayConverterWriter(classOf[LocalTime], a => Values.localTimeArray(a.asInstanceOf[Array[LocalTime]]))))
    case _: TimeValue => Converter(
      transform(new ArrayConverterWriter(classOf[OffsetTime], a => Values.timeArray(a.asInstanceOf[Array[OffsetTime]]))))
    case _: LocalDateTimeValue => Converter(
      transform(new ArrayConverterWriter(classOf[LocalDateTime], a => Values.localDateTimeArray(a.asInstanceOf[Array[LocalDateTime]]))))
    case _ => throw new CypherTypeException("Property values can only be of primitive types or arrays thereof")
  }
     */

    // This version is for maps of java values
//    @SuppressWarnings( "unchecked" )
//    public static Object mapGetProperty( Object object, String key )
//    {
//        try
//        {
//            Map<String,Object> map = (Map<String,Object>) object;
//            return map.get( key );
//        }
//        catch ( ClassCastException e )
//        {
//            throw new CypherTypeException( "Type mismatch: expected a map but was " + object, e );
//        }
//    }

    @SuppressWarnings( "unchecked" )
    public static Object mapGetProperty( Object object, String key )
    {
        try
        {
            MapValue map = (MapValue) object;
            return map.get( key );
        }
        catch ( ClassCastException e )
        {
            throw new CypherTypeException( "Type mismatch: expected a map but was " + object, e );
        }
    }

    static class ArrayIterator implements Iterator
    {
        private int position;
        private final int len;
        private final Object array;

        private ArrayIterator( Object array )
        {
            this.position = 0;
            this.len = Array.getLength( array );
            this.array = array;
        }

        @Override
        public boolean hasNext()
        {
            return position < len;
        }

        @Override
        public Object next()
        {
            if ( position >= len )
            {
                throw new NoSuchElementException();
            }
            int offset = position++;
            return Array.get( array, offset );
        }
    }
}
