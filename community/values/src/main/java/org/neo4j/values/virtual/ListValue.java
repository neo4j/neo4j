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
package org.neo4j.values.virtual;

import org.github.jamm.Unmetered;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Comparison;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;
import static org.neo4j.values.virtual.ArrayHelpers.assertValueRepresentation;
import static org.neo4j.values.virtual.ArrayHelpers.containsNull;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;

public abstract class ListValue extends VirtualValue implements SequenceValue, Iterable<AnyValue>
{
    private static final int NOT_MEMOIZED = -1;

    public abstract int size();

    public abstract ValueRepresentation itemValueRepresentation();

    @Override
    public abstract AnyValue value( int offset );

    @Override
    public String getTypeName()
    {
        return "List";
    }

    private static final long ARRAY_VALUE_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance( ArrayValueListValue.class );
    static final class ArrayValueListValue extends ListValue
    {
        private final ArrayValue array;

        ArrayValueListValue( ArrayValue array )
        {
            this.array = array;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return RANDOM_ACCESS;
        }

        @Override
        public ArrayValue toStorableArray()
        {
            return array;
        }

        @Override
        public int size()
        {
            return array.length();
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            return isEmpty() ? ValueRepresentation.UNKNOWN : head().valueRepresentation();
        }

        @Override
        public AnyValue value( int offset )
        {
            return array.value( offset );
        }

        @Override
        protected int computeHashToMemoize()
        {
            return array.hashCode();
        }

        @Override
        public long estimatedHeapUsage()
        {
            return ARRAY_VALUE_LIST_VALUE_SHALLOW_SIZE + array.estimatedHeapUsage();
        }
    }

    private static final long ARRAY_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance( ArrayListValue.class );
    static final class ArrayListValue extends ListValue
    {
        private final AnyValue[] values;
        private final long payloadSize;
        @Unmetered
        private final ValueRepresentation itemRepresentation;

        ArrayListValue( AnyValue[] values, long payloadSize, ValueRepresentation itemRepresentation )
        {
            assert values != null;
            this.payloadSize = shallowSizeOfObjectArray( values.length ) + payloadSize;
            assert !containsNull( values );
            assert assertValueRepresentation( values, itemRepresentation );

            this.values = values;
            this.itemRepresentation = itemRepresentation;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return RANDOM_ACCESS;
        }

        @Override
        public int size()
        {
            return values.length;
        }

        @Override
        public AnyValue value( int offset )
        {
            return values[offset];
        }

        @Override
        public AnyValue[] asArray()
        {
            return values;
        }

        @Override
        protected int computeHashToMemoize()
        {
            return Arrays.hashCode( values );
        }

        @Override
        public long estimatedHeapUsage()
        {
            return ARRAY_LIST_VALUE_SHALLOW_SIZE + payloadSize;
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            return itemRepresentation;
        }
    }

    private static final long JAVA_LIST_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance( JavaListListValue.class );
    static final class JavaListListValue extends ListValue
    {
        private final List<AnyValue> values;
        private final long payloadSize;
        @Unmetered
        private final ValueRepresentation itemRepresentation;

        JavaListListValue( List<AnyValue> values, long payloadSize, ValueRepresentation itemRepresentation )
        {
            this.payloadSize = payloadSize;
            assert values != null;
            assert !containsNull( values );
            assert assertValueRepresentation( values.toArray( AnyValue[]::new ), itemRepresentation );

            this.values = values;
            this.itemRepresentation = itemRepresentation;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return IterationPreference.ITERATION;
        }

        @Override
        public boolean isEmpty()
        {
            return values.isEmpty();
        }

        @Override
        public int size()
        {
            return values.size();
        }

        @Override
        public AnyValue value( int offset )
        {
            return values.get( offset );
        }

        @Override
        public AnyValue[] asArray()
        {
            return values.toArray( new AnyValue[0] );
        }

        @Override
        protected int computeHashToMemoize()
        {
            return values.hashCode();
        }

        @Override
        public Iterator<AnyValue> iterator()
        {
            return values.iterator();
        }

        @Override
        public long estimatedHeapUsage()
        {
            return JAVA_LIST_LIST_VALUE_SHALLOW_SIZE + payloadSize;
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            return itemRepresentation;
        }
    }

    private static final long LIST_SLICE_SHALLOW_SIZE = shallowSizeOfInstance( ListSlice.class );
    static final class ListSlice extends ListValue
    {
        private final ListValue inner;
        private final int from;
        private final int to;

        ListSlice( ListValue inner, int from, int to )
        {
            assert from >= 0;
            assert to <= inner.size();
            assert from <= to;
            this.inner = inner;
            this.from = from;
            this.to = to;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return inner.iterationPreference();
        }

        @Override
        public int size()
        {
            return to - from;
        }

        @Override
        public AnyValue value( int offset )
        {
            return inner.value( offset + from );
        }

        @Override
        public Iterator<AnyValue> iterator()
        {
            switch ( inner.iterationPreference() )
            {
            case RANDOM_ACCESS:
                return super.iterator();
            case ITERATION:
                return new PrefetchingIterator<>()
                {
                    private int count;
                    private Iterator<AnyValue> innerIterator = inner.iterator();

                    @Override
                    protected AnyValue fetchNextOrNull()
                    {
                        //make sure we are at least at first element
                        while ( count < from && innerIterator.hasNext() )
                        {
                            innerIterator.next();
                            count++;
                        }
                        //check if we are done
                        if ( count < from || count >= to || !innerIterator.hasNext() )
                        {
                            return null;
                        }
                        //take the next step
                        count++;
                        return innerIterator.next();
                    }
                };

            default:
                throw new IllegalStateException( "unknown iteration preference" );
            }
        }

        @Override
        public long estimatedHeapUsage()
        {
            return LIST_SLICE_SHALLOW_SIZE + inner.estimatedHeapUsage();
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            return inner.itemValueRepresentation();
        }
    }

    private static final long REVERSED_LIST_SHALLOW_SIZE = shallowSizeOfInstance( ReversedList.class );
    static final class ReversedList extends ListValue
    {
        private final ListValue inner;

        ReversedList( ListValue inner )
        {
            this.inner = inner;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return inner.iterationPreference();
        }

        @Override
        public int size()
        {
            return inner.size();
        }

        @Override
        public boolean isEmpty()
        {
            return inner.isEmpty();
        }

        @Override
        public AnyValue value( int offset )
        {
            return inner.value( size() - 1 - offset );
        }

        @Override
        public long estimatedHeapUsage()
        {
            return REVERSED_LIST_SHALLOW_SIZE + inner.estimatedHeapUsage();
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            return inner.itemValueRepresentation();
        }
    }

    private static final long INTEGRAL_RANGE_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance( IntegralRangeListValue.class );
    public static final class IntegralRangeListValue extends ListValue
    {
        private final long start;
        private final long end;
        private final long step;
        private int length = -1;

        IntegralRangeListValue( long start, long end, long step )
        {
            this.start = start;
            this.end = end;
            this.step = step;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return RANDOM_ACCESS;
        }

        @Override
        public String toString()
        {
            return "Range(" + start + "..." + end + ", step = " + step + ")";
        }

        @Override
        public int size()
        {
            if ( length == -1 )
            {
                long l = ((end - start) / step) + 1;
                if ( l > ArrayUtil.MAX_ARRAY_SIZE )
                {
                    throw new OutOfMemoryError( "Cannot index an collection of size " + l );
                }
                length = Math.max( (int) l, 0 );
            }
            return length;
        }

        @Override
        public AnyValue value( int offset )
        {
            if ( offset >= size() )
            {
                throw new IndexOutOfBoundsException();
            }
            else
            {
                return Values.longValue( start + offset * step );
            }
        }

        @Override
        protected int computeHashToMemoize()
        {
            int hashCode = 1;
            long current = start;
            int size = size();
            for ( int i = 0; i < size; i++, current += step )
            {
                hashCode = HASH_CONSTANT * hashCode + Long.hashCode( current );
            }
            return hashCode;
        }

        @Override
        public long estimatedHeapUsage()
        {
            return INTEGRAL_RANGE_LIST_VALUE_SHALLOW_SIZE;
        }

        @Override
        public ArrayValue toStorableArray()
        {
            long current = start;
            int size = size();
            long[] array = new long[size];
            for ( int i = 0; i < size; i++, current += step )
            {
                array[i] = current;
            }
            return Values.longArray( array );
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            return ValueRepresentation.INT64;
        }
    }

    private static final long CONCAT_LIST_SHALLOW_SIZE = shallowSizeOfInstance( ConcatList.class );
    static final class ConcatList extends ListValue
    {
        private final ListValue[] lists;
        private final ValueRepresentation itemValueRepresentation;
        private int size = -1;

        ConcatList( ListValue[] lists )
        {
            ValueRepresentation representation = null;
            for ( ListValue list : lists )
            {
                if ( list.nonEmpty() )
                {
                    representation = representation == null ? list.itemValueRepresentation() : representation.coerce( list.itemValueRepresentation() );
                }
            }
            this.itemValueRepresentation = representation;
            this.lists = lists;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return IterationPreference.ITERATION;
        }

        @Override
        public int size()
        {
            if ( size < 0 )
            {
                int s = 0;
                for ( ListValue list : lists )
                {
                    s += list.size();
                }
                size = s;
            }
            return size;
        }

        @Override
        public boolean isEmpty()
        {
            for ( ListValue list : lists )
            {
                if ( !list.isEmpty() )
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public AnyValue value( int offset )
        {
            for ( ListValue list : lists )
            {
                int size = list.size();
                if ( offset < size )
                {
                    return list.value( offset );
                }
                offset -= size;
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        public long estimatedHeapUsage()
        {
            long s = 0;
            for ( ListValue list : lists )
            {
                s += list.estimatedHeapUsage();
            }
            return CONCAT_LIST_SHALLOW_SIZE + s;
        }

        @Override
        public ListValue appendAll( ListValue value )
        {
            var newSize = lists.length + 1;
            var newArray = new ListValue[newSize];
            System.arraycopy( lists, 0, newArray, 0, lists.length );
            newArray[lists.length] = value;
            return new ConcatList( newArray );
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            return itemValueRepresentation;
        }
    }

    private static final long APPEND_LIST_SHALLOW_SIZE = shallowSizeOfInstance( AppendList.class );
    public static final class AppendList extends ListValue
    {
        private final ListValue base;
        private final AnyValue appended;
        private int size;

        AppendList( ListValue base, AnyValue appended )
        {
            this.base = base;
            this.appended = appended;
            this.size = NOT_MEMOIZED;
        }

        @Override
        public ArrayValue toStorableArray()
        {
            if ( base instanceof ArrayValueListValue )
            {
                ArrayValue array = ((ArrayValueListValue) base).array;
                if ( array.hasCompatibleType( appended ) )
                {
                    return array.copyWithAppended( appended );
                }
            }
            return super.toStorableArray();
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return base.iterationPreference();
        }

        @Override
        public int size()
        {
            if ( size == NOT_MEMOIZED )
            {
                size = base.size() + 1;
            }
            return size;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public AnyValue value( int offset )
        {
            int size = base.size();
            if ( offset < size )
            {
                return base.value( offset );
            }
            else if ( offset < size + 1 )
            {
                return appended;
            }
            else
            {
                throw new IndexOutOfBoundsException( offset + " is outside range " + size );
            }
        }

        @Override
        public Iterator<AnyValue> iterator()
        {
            switch ( base.iterationPreference() )
            {
            case RANDOM_ACCESS:
                return super.iterator();
            case ITERATION:
                return Iterators.appendTo( base.iterator(), appended );
            default:
                throw new IllegalStateException( "unknown iteration preference" );
            }
        }

        @Override
        public long estimatedHeapUsage()
        {
            return APPEND_LIST_SHALLOW_SIZE + base.estimatedHeapUsage() + appended.estimatedHeapUsage();
        }

        public long estimatedAppendedHeapUsage()
        {
            return APPEND_LIST_SHALLOW_SIZE + appended.estimatedHeapUsage();
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            if ( base.isEmpty() )
            {
                return appended.valueRepresentation();
            }
            else
            {
                return base.itemValueRepresentation().coerce( appended.valueRepresentation() );
            }
        }
    }

    private static final long PREPEND_LIST_SHALLOW_SIZE = shallowSizeOfInstance( PrependList.class );
    static final class PrependList extends ListValue
    {
        private final ListValue base;
        private final AnyValue prepended;
        private int size;

        PrependList( ListValue base, AnyValue prepended )
        {
            this.base = base;
            this.prepended = prepended;
            this.size = NOT_MEMOIZED;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return base.iterationPreference();
        }

        @Override
        public int size()
        {
            if ( size == NOT_MEMOIZED )
            {
                size = base.size() + 1;
            }
            return size;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public AnyValue value( int offset )
        {
            int size = base.size();
            if ( offset < 1 )
            {
                return prepended;
            }
            else if ( offset < size + 1 )
            {
                return base.value( offset - 1 );
            }
            else
            {
                throw new IndexOutOfBoundsException( offset + " is outside range " + size );
            }
        }

        @Override
        public Iterator<AnyValue> iterator()
        {
            switch ( base.iterationPreference() )
            {
            case RANDOM_ACCESS:
                return super.iterator();
            case ITERATION:
                return Iterators.prependTo( base.iterator(), prepended );
            default:
                throw new IllegalStateException( "unknown iteration preference" );
            }
        }

        @Override
        public long estimatedHeapUsage()
        {
            return PREPEND_LIST_SHALLOW_SIZE + base.estimatedHeapUsage() + prepended.estimatedHeapUsage();
        }

        @Override
        public ArrayValue toStorableArray()
        {
            if ( base instanceof ArrayValueListValue )
            {
                ArrayValue array = ((ArrayValueListValue) base).array;
                if ( array.hasCompatibleType( prepended ) )
                {
                    return array.copyWithPrepended( prepended );
                }
            }
            return super.toStorableArray();
        }

        @Override
        public ValueRepresentation itemValueRepresentation()
        {
            if ( base.isEmpty() )
            {
                return prepended.valueRepresentation();
            }
            else
            {
                return base.itemValueRepresentation().coerce( prepended.valueRepresentation() );
            }
        }
    }

    public boolean nonEmpty()
    {
        return !isEmpty();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder().append( getTypeName() ).append( '{' );
        int i = 0;
        for ( ; i < size() - 1; i++ )
        {
            sb.append( value( i ) );
            sb.append( ", " );
        }
        if ( size() > 0 )
        {
            sb.append( value( i ) );
        }
        sb.append( '}' );
        return sb.toString();
    }

    @Override
    public boolean isSequenceValue()
    {
        return true;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapSequence( this );
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        return other != null && other.isSequenceValue() && equals( (SequenceValue) other );
    }

    public AnyValue head()
    {
        int size = size();
        if ( size == 0 )
        {
            throw new NoSuchElementException( "head of empty list" );
        }
        return value( 0 );
    }

    public AnyValue last()
    {
        int size = size();
        if ( size == 0 )
        {
            throw new NoSuchElementException( "last of empty list" );
        }
        return value( size - 1 );
    }

    @Override
    public Iterator<AnyValue> iterator()
    {
        return new Iterator<>()
        {
            private int count;

            @Override
            public boolean hasNext()
            {
                return count < size();
            }

            @Override
            public AnyValue next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return value( count++ );
            }
        };
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.LIST;
    }

    @Override
    public int length()
    {
        return size();
    }

    @Override
    public int unsafeCompareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        ListValue otherList = (ListValue) other;
        return compareToSequence( otherList, comparator );
    }

    @Override
    public Comparison unsafeTernaryCompareTo( VirtualValue other, TernaryComparator<AnyValue> comparator )
    {
        ListValue otherList = (ListValue) other;
        return ternaryCompareToSequence( otherList, comparator );
    }

    public AnyValue[] asArray()
    {
        switch ( iterationPreference() )
        {
        case RANDOM_ACCESS:
            return randomAccessAsArray();
        case ITERATION:
            return iterationAsArray();
        default:
            throw new IllegalStateException( "not a valid iteration preference" );
        }
    }

    @Override
    protected int computeHashToMemoize()
    {
        switch ( iterationPreference() )
        {
        case RANDOM_ACCESS:
            return randomAccessComputeHash();
        case ITERATION:
            return iterationComputeHash();
        default:
            throw new IllegalStateException( "not a valid iteration preference" );
        }
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        switch ( iterationPreference() )
        {
        case RANDOM_ACCESS:
            randomAccessWriteTo( writer );
            break;
        case ITERATION:
            iterationWriteTo( writer );
            break;
        default:
            throw new IllegalStateException( "not a valid iteration preference" );
        }
    }

    public ListValue slice( int from, int to )
    {
        int f = Math.max( from, 0 );
        int t = Math.min( to, size() );
        if ( f > t )
        {
            return EMPTY_LIST;
        }
        else
        {
            return new ListSlice( this, f, t );
        }
    }

    public ListValue tail()
    {
        return slice( 1, size() );
    }

    public ListValue drop( int n )
    {
        int size = size();
        int start = Math.max( 0, Math.min( n, size ) );
        return new ListSlice( this, start, size );
    }

    public ListValue take( int n )
    {
        int end = Math.max( 0, Math.min( n, size() ) );
        return new ListSlice( this, 0, end );
    }

    public ListValue reverse()
    {
        return new ReversedList( this );
    }

    public AppendList append( AnyValue value )
    {
        return new AppendList( this, value );
    }

    public ListValue prepend( AnyValue value )
    {
        return new PrependList( this, value );
    }

    public ListValue appendAll( ListValue value )
    {
        return new ConcatList( new ListValue[]{this, value} );
    }

    public ListValue distinct()
    {
        long keptValuesHeapSize = 0;
        Set<AnyValue> seen = new HashSet<>();
        List<AnyValue> kept = new ArrayList<>();
        ValueRepresentation representation = null;
        for ( AnyValue value : this )
        {
            if ( seen.add( value ) )
            {
                kept.add( value );
                keptValuesHeapSize += value.estimatedHeapUsage();
            }
            representation = representation == null ? value.valueRepresentation() : representation.coerce( value.valueRepresentation() );
        }
        return new JavaListListValue( kept, keptValuesHeapSize, representation == null ? ValueRepresentation.UNKNOWN : representation );
    }

    public ArrayValue toStorableArray()
    {
        if ( isEmpty() )
        {
            return Values.EMPTY_TEXT_ARRAY;
        }
        else
        {
            return itemValueRepresentation().arrayOf( this );
        }
    }

    private AnyValue[] iterationAsArray()
    {
        List<AnyValue> values = new ArrayList<>();
        int size = 0;
        for ( AnyValue value : this )
        {
            values.add( value );
            size++;
        }
        return values.toArray( new AnyValue[size] );
    }

    private AnyValue[] randomAccessAsArray()
    {
        int size = size();
        AnyValue[] values = new AnyValue[size];
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = value( i );
        }
        return values;
    }

    private int randomAccessComputeHash()
    {
        int hashCode = 1;
        int size = size();
        for ( int i = 0; i < size; i++ )
        {
            hashCode = HASH_CONSTANT * hashCode + value( i ).hashCode();
        }
        return hashCode;
    }

    private int iterationComputeHash()
    {
        int hashCode = 1;
        for ( AnyValue value : this )
        {
            hashCode = HASH_CONSTANT * hashCode + value.hashCode();
        }
        return hashCode;
    }

    private <E extends Exception> void randomAccessWriteTo( AnyValueWriter<E> writer ) throws E
    {
        writer.beginList( size() );
        for ( int i = 0; i < size(); i++ )
        {
            value( i ).writeTo( writer );
        }
        writer.endList();
    }

    private <E extends Exception> void iterationWriteTo( AnyValueWriter<E> writer ) throws E
    {
        writer.beginList( size() );
        for ( AnyValue value : this )
        {
            value.writeTo( writer );
        }
        writer.endList();
    }

}
