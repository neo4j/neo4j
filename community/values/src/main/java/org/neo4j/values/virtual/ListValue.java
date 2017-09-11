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
package org.neo4j.values.virtual;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.NumberValues;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.virtual.ArrayHelpers.containsNull;

public abstract class ListValue extends VirtualValue implements SequenceValue, Iterable<AnyValue>
{
    public abstract int size();

    public abstract AnyValue value( int offset );

    public abstract AnyValue[] asArray();

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
        StringBuilder sb = new StringBuilder( "List{" );
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
    public boolean equals( VirtualValue other )
    {
        if ( other == null || !other.isSequenceValue() )
        {
            return false;
        }
        return equals( (SequenceValue) other );
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
        return new Iterator<AnyValue>()
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
            return IterationPreference.RANDOM_ACCESS;
        }

        @Override
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            int length = array.length();
            writer.beginList( length );
            for ( int i = 0; i < length; i++ )
            {
                array.value( i ).writeTo( writer );
            }
            writer.endList();
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
        public AnyValue[] asArray()
        {
            int size = size();
            AnyValue[] values = new AnyValue[size];
            for ( int i = 0; i < size; i++ )
            {
                values[i] = array.value( i );
            }

            return values;
        }

        @Override
        public int computeHash()
        {
            return array.hashCode();
        }
    }

    static final class ArrayListValue extends ListValue
    {
        private final AnyValue[] values;

        ArrayListValue( AnyValue[] values )
        {
            assert values != null;
            assert !containsNull( values );

            this.values = values;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return IterationPreference.RANDOM_ACCESS;
        }

        @Override
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            writer.beginList( values.length );
            for ( AnyValue value : values )
            {
                value.writeTo( writer );
            }
            writer.endList();
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
    }

    static final class JavaListListValue extends ListValue
    {
        private final List<AnyValue> values;

        JavaListListValue( List<AnyValue> values )
        {
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
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            writer.beginList( values.size() );
            for ( AnyValue value : values )
            {
                value.writeTo( writer );
            }
            writer.endList();
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
            return values.toArray( new AnyValue[values.size()] );
        }

        @Override
        public int computeHash()
        {
            return values.hashCode();
        }
    }

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
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            writer.beginList( size() );
            for ( int i = from; i < to; i++ )
            {
                inner.value( i ).writeTo( writer );
            }
            writer.endList();
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
        public AnyValue[] asArray()
        {
            int len = size();
            AnyValue[] anyValues = new AnyValue[len];
            int index = 0;
            for ( int i = from; i < to; i++ )
            {
                anyValues[index++] = inner.value( i );
            }
            return anyValues;
        }

        @Override
        public int computeHash()
        {
            int hashCode = 1;
            for ( int i = from; i < to; i++ )
            {
                hashCode = 31 * hashCode + inner.value( i ).hashCode();
            }
            return hashCode;
        }
    }

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
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            writer.beginList( size() );
            for ( int i = inner.size() - 1; i >= 0; i-- )
            {
                inner.value( i ).writeTo( writer );
            }
            writer.endList();
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
        public AnyValue[] asArray()
        {
            int len = size();
            AnyValue[] anyValues = new AnyValue[len];
            for ( int i = 0; i < len; i++ )
            {
                anyValues[i] = value( i );
            }
            return anyValues;
        }

        @Override
        public int computeHash()
        {
            int hashCode = 1;
            for ( int i = inner.size() - 1; i >= 0; i-- )
            {
                hashCode = 31 * hashCode + inner.value( i ).hashCode();
            }
            return hashCode;
        }
    }

    static final class TransformedListValue extends ListValue
    {
        private final ListValue inner;
        private final Function<AnyValue,AnyValue> transform;

        TransformedListValue( ListValue inner, Function<AnyValue,AnyValue> transform )
        {
            this.inner = inner;
            this.transform = transform;
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return inner.iterationPreference();
        }

        @Override
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            writer.beginList( size() );
            for ( int i = 0; i < inner.size(); i++ )
            {
                transform.apply( inner.value( i ) ).writeTo( writer );
            }
            writer.endList();
        }

        @Override
        public int size()
        {
            return inner.size();
        }

        @Override
        public AnyValue value( int offset )
        {
            return transform.apply( inner.value( offset ) );
        }

        @Override
        public AnyValue[] asArray()
        {
            int len = size();
            AnyValue[] anyValues = new AnyValue[len];
            for ( int i = 0; i < len; i++ )
            {
                anyValues[i] = transform.apply( inner.value( i ) );
            }
            return anyValues;
        }

        @Override
        public int computeHash()
        {
            int hashCode = 1;
            for ( int i = 0; i < size(); i++ )
            {
                hashCode = 31 * hashCode + transform.apply( inner.value( i ) ).hashCode();
            }
            return hashCode;
        }
    }

    static final class FilteredListValue extends ListValue
    {
        private final ListValue inner;
        private final Function<AnyValue,Boolean> filter;
        private int size = -1;

        FilteredListValue( ListValue inner, Function<AnyValue,Boolean> filter )
        {
            this.inner = inner;
            this.filter = filter;
        }

        @Override
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            writer.beginList( size() );
            for ( int i = 0; i < inner.size(); i++ )
            {
                AnyValue value = inner.value( i );
                if ( filter.apply( value ) )
                {
                    value.writeTo( writer );
                }
            }
            writer.endList();
        }

        @Override
        public int size()
        {
            if ( size < 0 )
            {

                int s = 0;
                for ( int i = 0; i < inner.size(); i++ )
                {
                    if ( filter.apply( inner.value( i ) ) )
                    {
                        s++;
                    }
                }
                size = s;
            }

            return size;
        }

        @Override
        public AnyValue value( int offset )
        {
            int actualOffset = 0;
            int size = inner.size();
            for ( int i = 0; i < size; i++ )
            {
                AnyValue value = inner.value( i );
                if ( filter.apply( value ) )
                {
                    if ( actualOffset == offset )
                    {
                        return value;
                    }
                    actualOffset++;
                }
            }

            throw new IndexOutOfBoundsException();
        }

        @Override
        public AnyValue[] asArray()
        {
            int len = size();
            AnyValue[] anyValues = new AnyValue[len];
            int index = 0;
            for ( int i = 0; i < inner.size(); i++ )
            {
                AnyValue value = inner.value( i );
                if ( filter.apply( value ) )
                {
                    anyValues[index++] = value;
                }
            }
            return anyValues;
        }

        @Override
        public int computeHash()
        {
            int hashCode = 1;
            for ( int i = 0; i < inner.size(); i++ )
            {
                AnyValue value = inner.value( i );
                if ( filter.apply( value ) )
                {
                    hashCode = 31 * hashCode + value.hashCode();
                }
            }
            return hashCode;
        }

        @Override
        public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
        {
            if ( !(other instanceof ListValue) )
            {
                throw new IllegalArgumentException( "Cannot compare different virtual values" );
            }
            ListValue otherList = (ListValue) other;
            Iterator<AnyValue> thisIterator = iterator();
            Iterator<AnyValue> thatIterator = otherList.iterator();
            while ( thisIterator.hasNext() )
            {
                if ( !thatIterator.hasNext() )
                {
                    return 1;
                }
                int compare = comparator.compare( thisIterator.next(), thatIterator.next() );
                if ( compare != 0 )
                {
                    return compare;
                }
            }
            if ( thatIterator.hasNext() )
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }

        @Override
        public Iterator<AnyValue> iterator()
        {
            return new FilteredIterator();
        }

        @Override
        public IterationPreference iterationPreference()
        {
            return IterationPreference.ITERATION;
        }

        private class FilteredIterator implements Iterator<AnyValue>
        {
            private AnyValue next;
            private int index;

            FilteredIterator()
            {
                computeNext();
            }

            @Override
            public boolean hasNext()
            {
                return next != null;
            }

            @Override
            public AnyValue next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }

                AnyValue current = next;
                computeNext();
                return current;
            }

            private void computeNext()
            {
                if ( index >= inner.size() )
                {
                    next = null;
                }
                else
                {
                    while ( true )
                    {
                        if ( index >= inner.size() )
                        {
                            next = null;
                            return;
                        }
                        AnyValue candidate = inner.value( index++ );
                        if ( filter.apply( candidate ) )
                        {
                            next = candidate;
                            return;
                        }
                    }
                }
            }
        }
    }

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
            return IterationPreference.RANDOM_ACCESS;
        }

        @Override
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            int size = size();
            writer.beginList( size );
            for ( long current = start; check( current ); current += step )
            {
                Values.longValue( current ).writeTo( writer );
            }
            writer.endList();

        }

        @Override
        public String toString()
        {
            return "Range(" + start + "..." + end + ", step = " + step + ")";
        }

        private boolean check( long current )
        {
            if ( step > 0 )
            {
                return current <= end;
            }
            else
            {
                return current >= end;
            }
        }

        @Override
        public int size()
        {
            if ( length != -1 )
            {
                return length;
            }
            else
            {
                long l = ((end - start) / step) + 1;
                if ( l > Integer.MAX_VALUE )
                {
                    throw new OutOfMemoryError( "Cannot index an collection of size " + l );
                }
                length = (int) l;
                return length;
            }
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
        public AnyValue[] asArray()
        {
            int len = size();
            AnyValue[] anyValues = new AnyValue[len];
            int i = 0;
            for ( long current = start; check( current ); current += step, i++ )
            {
                anyValues[i] = Values.longValue( current );
            }
            return anyValues;
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

    }

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
        public AnyValue[] asArray()
        {
            AnyValue[] values = new AnyValue[size()];
            int start = 0;
            for ( ListValue list : lists )
            {
                int length = list.length();
                System.arraycopy( list.asArray(), 0, values, start, length );
                start += length;
            }
            return values;
        }

        @Override
        public int computeHash()
        {
            int hashCode = 1;
            int size = size();
            for ( int i = 0; i < size; i++ )
            {
                hashCode = 31 * hashCode + value( i ).hashCode();
            }
            return hashCode;
        }

        @Override
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            writer.beginList( size() );
            for ( int i = 0; i < size(); i++ )
            {
                value( i ).writeTo( writer );
            }
            writer.endList();
        }
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.LIST;
    }

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( !(other instanceof ListValue) )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }
        //more efficient to use another implementation here
        if ( other instanceof FilteredListValue )
        {
            return -other.compareTo( this, comparator );
        }
        ListValue otherList = (ListValue) other;
        int x = Integer.compare( this.length(), otherList.length() );

        if ( x == 0 )
        {
            for ( int i = 0; i < length(); i++ )
            {
                x = comparator.compare( this.value( i ), otherList.value( i ) );
                if ( x != 0 )
                {
                    return x;
                }
            }
        }

        return x;
    }

    @Override
    public int length()
    {
        return size();
    }
}
