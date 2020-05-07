/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.values.storable.Values;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.virtual.ArrayHelpers.containsNull;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;

public abstract class ListValue extends VirtualValue implements SequenceValue, Iterable<AnyValue>
{
    public abstract int size();

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
        public boolean storable()
        {
            return true;
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
        public AnyValue value( int offset )
        {
            return array.value( offset );
        }

        @Override
        public int computeHash()
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

        ArrayListValue( AnyValue[] values, long payloadSize )
        {
            this.payloadSize = shallowSizeOfObjectArray( values.length ) + payloadSize;
            assert values != null;
            assert !containsNull( values );

            this.values = values;
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
        public int computeHash()
        {
            return Arrays.hashCode( values );
        }

        @Override
        public long estimatedHeapUsage()
        {
            return ARRAY_LIST_VALUE_SHALLOW_SIZE + payloadSize;
        }
    }

    private static final long JAVA_LIST_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance( JavaListListValue.class );
    static final class JavaListListValue extends ListValue
    {
        private final List<AnyValue> values;
        private final long payloadSize;

        JavaListListValue( List<AnyValue> values, long payloadSize )
        {
            this.payloadSize = payloadSize;
            assert values != null;
            assert !containsNull( values );

            this.values = values;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return IterationPreference.ITERATION;
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
        public int computeHash()
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
        public AnyValue value( int offset )
        {
            return inner.value( size() - 1 - offset );
        }

        @Override
        public long estimatedHeapUsage()
        {
            return REVERSED_LIST_SHALLOW_SIZE + inner.estimatedHeapUsage();
        }
    }

    private static final long INTEGRAL_RANGE_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance( IntegralRangeListValue.class );
    static final class IntegralRangeListValue extends ListValue
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
        public int computeHash()
        {
            int hashCode = 1;
            long current = start;
            int size = size();
            for ( int i = 0; i < size; i++, current += step )
            {
                hashCode = 31 * hashCode + Long.hashCode( current );
            }
            return hashCode;
        }

        @Override
        public long estimatedHeapUsage()
        {
            return INTEGRAL_RANGE_LIST_VALUE_SHALLOW_SIZE;
        }
    }

    private static final long CONCAT_LIST_SHALLOW_SIZE = shallowSizeOfInstance( ConcatList.class );
    static final class ConcatList extends ListValue
    {
        private final ListValue[] lists;
        private int size = -1;

        ConcatList( ListValue[] lists )
        {
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
            int s = 0;
            for ( ListValue list : lists )
            {
                s += list.estimatedHeapUsage();
            }
            return CONCAT_LIST_SHALLOW_SIZE + s;
        }
    }

    private static final long APPEND_LIST_SHALLOW_SIZE = shallowSizeOfInstance( AppendList.class );
    static final class AppendList extends ListValue
    {
        private final ListValue base;
        private final AnyValue appended;

        AppendList( ListValue base, AnyValue appended )
        {
            this.base = base;
            this.appended = appended;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return base.iterationPreference();
        }

        @Override
        public int size()
        {
            return base.size() + 1;
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
    }

    private static final long PREPEND_LIST_SHALLOW_SIZE = shallowSizeOfInstance( PrependList.class );
    static final class PrependList extends ListValue
    {
        private final ListValue base;
        private final AnyValue prepended;

        PrependList( ListValue base, AnyValue prepended )
        {
            this.base = base;
            this.prepended = prepended;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return base.iterationPreference();
        }

        @Override
        public int size()
        {
            return 1 + base.size();
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
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public boolean nonEmpty()
    {
        return size() != 0;
    }

    public boolean storable()
    {
        return false;
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

    public ArrayValue toStorableArray()
    {
        throw new UnsupportedOperationException( "List cannot be turned into a storable array" );
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
    public int computeHash()
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

    public ListValue append( AnyValue value )
    {
        return new AppendList( this, value );
    }

    public ListValue prepend( AnyValue value )
    {
        return new PrependList( this, value );
    }

    public ListValue distinct()
    {
        long keptValuesHeapSize = 0;
        Set<AnyValue> seen = new HashSet<>();
        List<AnyValue> kept = new ArrayList<>();
        for ( AnyValue value : this )
        {
            if ( seen.add( value ) )
            {
                kept.add( value );
                keptValuesHeapSize += value.estimatedHeapUsage();
            }
        }
        return new JavaListListValue( kept, keptValuesHeapSize );
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
            hashCode = 31 * hashCode + value( i ).hashCode();
        }
        return hashCode;
    }

    private int iterationComputeHash()
    {
        int hashCode = 1;
        for ( AnyValue value : this )
        {
            hashCode = 31 * hashCode + value.hashCode();
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
