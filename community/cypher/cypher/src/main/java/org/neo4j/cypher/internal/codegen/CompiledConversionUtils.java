/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.codegen;

import java.lang.reflect.Array;
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

import org.neo4j.cypher.internal.util.v3_4.CypherTypeException;
import org.neo4j.cypher.internal.util.v3_4.IncomparableValuesException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.NodeProxyWrappingNodeValue;
import org.neo4j.kernel.impl.util.RelationshipProxyWrappingValue;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
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

    public static Set<?> toSet( Object value )
    {
        if ( value == null || value == NO_VALUE )
        {
            return Collections.emptySet();
        }
        else if ( value instanceof SequenceValue )
        {
            SequenceValue sequenceValue = (SequenceValue) value;
            Iterator<AnyValue> iterator = sequenceValue.iterator();
            Set<AnyValue> set;
            if ( sequenceValue.iterationPreference() == RANDOM_ACCESS )
            {
                // If we have a random access sequence value length() should be cheap and we can optimize the initial capacity
                int length = sequenceValue.length();
                set = new HashSet<>( length );
                for ( int i = 0; i < length; i++ )
                {
                    set.add( sequenceValue.value( i ) );
                }
            }
            else
            {
                set = new HashSet<>();
                while ( iterator.hasNext() )
                {
                    AnyValue element = iterator.next();
                    set.add( element );
                }
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
        if ( lhs == null || rhs == null || lhs == NO_VALUE || rhs == NO_VALUE )
        {
            return null;
        }

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

    // Ternary OR
    public static Boolean or( Object lhs, Object rhs )
    {
        Boolean l = toBooleanOrNull( lhs );
        Boolean r = toBooleanOrNull( rhs );

        if ( l == null && r == null )
        {
            return null;
        }
        else if ( l == null )
        {
            return r ? true : null;
        }
        else if ( r == null )
        {
            return l ? true : null;
        }
        return l || r;
    }

    // Ternary NOT
    public static Boolean not( Object predicate )
    {
        Boolean b = toBooleanOrNull( predicate );
        if ( b == null )
        {
            return null;
        }
        return !b;
    }

    private static Boolean toBooleanOrNull( Object o )
    {
        if ( o == null || o == NO_VALUE )
        {
            return null;
        }
        else if ( o instanceof Boolean )
        {
            return (Boolean) o;
        }
        else if ( o instanceof BooleanValue )
        {
            return ((BooleanValue) o).booleanValue();
        }
        throw new CypherTypeException( "Don't know how to treat that as a boolean: " + o.toString(), null );
    }

    @SuppressWarnings( {"unchecked", "WeakerAccess"} )
    public static AnyValue materializeAnyResult( EmbeddedProxySPI proxySpi, Object anyValue )
    {
        if ( anyValue == null || anyValue == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( anyValue instanceof AnyValue )
        {
            return materializeAnyValueResult( proxySpi, anyValue );
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
                return VirtualValues.list( copy );
            }
        }
        else
        {
            return ValueUtils.of( anyValue );
        }
    }

    // NOTE: This assumes anyValue is an instance of AnyValue
    public static AnyValue materializeAnyValueResult( EmbeddedProxySPI proxySpi, Object anyValue )
    {
        if ( anyValue instanceof VirtualNodeValue )
        {
            if ( anyValue instanceof NodeValue )
            {
                return (AnyValue) anyValue;
            }
            return ValueUtils.fromNodeProxy( proxySpi.newNodeProxy( ((VirtualNodeValue) anyValue).id() ) );
        }
        if ( anyValue instanceof VirtualRelationshipValue )
        {
            if ( anyValue instanceof RelationshipValue )
            {
                return (AnyValue) anyValue;
            }
            return ValueUtils.fromRelationshipProxy( proxySpi.newRelationshipProxy( ((VirtualRelationshipValue) anyValue).id() ) );
        }
        // If it is a list or map, run it through a ValueMapper that will create proxy objects for entities if needed.
        // This will first do a dry run and return as it is if no conversion is needed.
        // If in the future we will always create proxy objects directly whenever we create values we can skip this
        // Doing this conversion lazily instead, by wrapping with TransformedListValue or TransformedMapValue is probably not a
        // good idea because of the complexities involved (see TOMBSTONE in VirtualValues about why TransformedListValue was killed).
        // NOTE: There is also a case where a ListValue can be storable (ArrayValueListValue) where no conversion is needed
        if ( (anyValue instanceof ListValue && !((ListValue) anyValue).storable()) || anyValue instanceof MapValue )
        {
            return CompiledMaterializeValueMapper.mapAnyValue( proxySpi, (AnyValue) anyValue );
        }
        return (AnyValue) anyValue;
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
        else if ( anyValue instanceof Node )
        {
            return ValueUtils.fromNodeProxy( (Node) anyValue );
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
        else if ( anyValue instanceof Relationship )
        {
            return ValueUtils.fromRelationshipProxy( (Relationship) anyValue );
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
        else if ( iterable == null || iterable == NO_VALUE )
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
    public static long unboxNodeOrNull( VirtualNodeValue value )
    {
        if ( value == null )
        {
            return -1L;
        }
        return value.id();
    }

    @SuppressWarnings( "unused" ) // called from compiled code
    public static long unboxRelationshipOrNull( VirtualRelationshipValue value )
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
        if ( obj == null || obj == NO_VALUE )
        {
            return -1L;
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

    //In the store we only support storable Value types and arrays thereof.
    //In cypher we must make an effort to transform Cypher lists to appropriate arrays whenever
    //we are using sending values down to the store or to an index.
    public static Object makeValueNeoSafe( Object object )
    {
        AnyValue value = object instanceof AnyValue ? ((AnyValue) object) : ValueUtils.of( object );
        return org.neo4j.cypher.internal.runtime.interpreted.makeValueNeoSafe.apply( value );
    }

    @SuppressWarnings( "unchecked" )
    public static Object mapGetProperty( Object object, String key )
    {
        if ( object == NO_VALUE )
        {
            return NO_VALUE;
        }
        if ( object instanceof MapValue )
        {
            MapValue map = (MapValue) object;
            return map.get( key );
        }
        if ( object instanceof NodeProxyWrappingNodeValue )
        {
            return Values.of( ((NodeProxyWrappingNodeValue) object).nodeProxy().getProperty( key ) );
        }
        if ( object instanceof RelationshipProxyWrappingValue )
        {
            return Values.of( ((RelationshipProxyWrappingValue) object).relationshipProxy().getProperty( key ) );
        }
        if ( object instanceof PropertyContainer ) // Entity that is not wrapped by an AnyValue
        {
            return Values.of( ((PropertyContainer) object).getProperty( key ) );
        }
        if ( object instanceof NodeValue )
        {
            return ((NodeValue) object).properties().get( key );
        }
        if ( object instanceof RelationshipValue )
        {
            return ((RelationshipValue) object).properties().get( key );
        }
        if ( object instanceof Map<?,?> )
        {
            Map<String,Object> map = (Map<String,Object>) object;
            return map.get( key );
        }
        if ( object instanceof TemporalValue<?,?> )
        {
            return ((TemporalValue<?,?>) object).get( key );
        }
        if ( object instanceof DurationValue )
        {
            return ((DurationValue) object).get( key );
        }
        if ( object instanceof PointValue )
        {
            return ((PointValue) object).get( key );
        }

        // NOTE: VirtualNodeValue and VirtualRelationshipValue will fall through to here
        // To handle these we would need specialized cursor code
        throw new CypherTypeException( String.format( "Type mismatch: expected a map but was %s", object ), null );
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
