/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.storageengine.api.txstate;

import org.eclipse.collections.api.LazyLongIterable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.bag.primitive.MutableLongBag;
import org.eclipse.collections.api.block.function.primitive.LongToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.LongToByteFunction;
import org.eclipse.collections.api.block.function.primitive.LongToCharFunction;
import org.eclipse.collections.api.block.function.primitive.LongToDoubleFunction;
import org.eclipse.collections.api.block.function.primitive.LongToFloatFunction;
import org.eclipse.collections.api.block.function.primitive.LongToIntFunction;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.LongToShortFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectLongToObjectFunction;
import org.eclipse.collections.api.block.predicate.primitive.LongPredicate;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.collection.primitive.MutableBooleanCollection;
import org.eclipse.collections.api.collection.primitive.MutableByteCollection;
import org.eclipse.collections.api.collection.primitive.MutableCharCollection;
import org.eclipse.collections.api.collection.primitive.MutableDoubleCollection;
import org.eclipse.collections.api.collection.primitive.MutableFloatCollection;
import org.eclipse.collections.api.collection.primitive.MutableIntCollection;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.collection.primitive.MutableShortCollection;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.nio.LongBuffer;
import java.util.Collection;
import java.util.LongSummaryStatistics;

import org.neo4j.collection.RangeLongIterator;

public class RichMutableLongHashSet implements RichMutableLongSet
{
    private final MutableLongSet delegate;

    public RichMutableLongHashSet()
    {
        this( new LongHashSet() );
    }

    public RichMutableLongHashSet( MutableLongSet delegate )
    {
        this.delegate = delegate;
    }

    public static LongHashSet newSet( LongIterable source )
    {
        return LongHashSet.newSet( source );
    }

    public static LongHashSet newSetWith( long... source )
    {
        return LongHashSet.newSetWith( source );
    }

    @Override
    public int hashCode()
    {
        return delegate.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        return delegate.equals( obj );
    }

    @Override
    public int size()
    {
        return delegate.size();
    }

    @Override
    public void appendString( Appendable appendable, String start, String separator, String end )
    {
        delegate.appendString( appendable, start, separator, end );
    }

    @Override
    public boolean add( long element )
    {
        return delegate.add( element );
    }

    @Override
    public boolean addAll( long... source )
    {
        return delegate.addAll( source );
    }

    @Override
    public boolean addAll( LongIterable source )
    {
        return delegate.addAll( source );
    }

    @Override
    public boolean remove( long value )
    {
        return delegate.remove( value );
    }

    @Override
    public boolean removeAll( LongIterable source )
    {
        return delegate.removeAll( source );
    }

    @Override
    public boolean removeAll( long... source )
    {
        return delegate.removeAll( source );
    }

    @Override
    public boolean retainAll( LongIterable source )
    {
        return delegate.retainAll( source );
    }

    @Override
    public boolean retainAll( long... source )
    {
        return delegate.retainAll( source );
    }

    @Override
    public void clear()
    {
        delegate.clear();
    }

    @Override
    public MutableLongSet with( long element )
    {
        return delegate.with( element );
    }

    @Override
    public MutableLongSet without( long element )
    {
        return delegate.without( element );
    }

    @Override
    public MutableLongSet withAll( LongIterable elements )
    {
        return delegate.withAll( elements );
    }

    @Override
    public MutableLongSet withoutAll( LongIterable elements )
    {
        return delegate.withoutAll( elements );
    }

    @Override
    public MutableLongSet asUnmodifiable()
    {
        return delegate.asUnmodifiable();
    }

    @Override
    public MutableLongSet asSynchronized()
    {
        return delegate.asSynchronized();
    }

    @Override
    public ImmutableLongSet toImmutable()
    {
        return delegate.toImmutable();
    }

    @Override
    public MutableLongIterator longIterator()
    {
        return delegate.longIterator();
    }

    @Override
    public long[] toArray()
    {
        return delegate.toArray();
    }

    @Override
    public boolean contains( long value )
    {
        return delegate.contains( value );
    }

    @Override
    public void forEach( LongProcedure procedure )
    {
        delegate.forEach( procedure );
    }

    @Override
    public void each( LongProcedure procedure )
    {
        delegate.each( procedure );
    }

    @Override
    public MutableLongSet select( LongPredicate predicate )
    {
        return delegate.select( predicate );
    }

    @Override
    public <R extends MutableLongCollection> R select(
            LongPredicate predicate, R target )
    {
        return delegate.select( predicate, target );
    }

    @Override
    public MutableLongSet reject( LongPredicate predicate )
    {
        return delegate.reject( predicate );
    }

    @Override
    public <R extends MutableLongCollection> R reject(
            LongPredicate predicate, R target )
    {
        return delegate.reject( predicate, target );
    }

    @Override
    public <V> MutableSet<V> collect(
            LongToObjectFunction<? extends V> function )
    {
        return delegate.collect( function );
    }

    @Override
    public <V, R extends Collection<V>> R collect(
            LongToObjectFunction<? extends V> function, R target )
    {
        return delegate.collect( function, target );
    }

    @Override
    public long detectIfNone( LongPredicate predicate, long ifNone )
    {
        return delegate.detectIfNone( predicate, ifNone );
    }

    @Override
    public int count( LongPredicate predicate )
    {
        return delegate.count( predicate );
    }

    @Override
    public boolean anySatisfy( LongPredicate predicate )
    {
        return delegate.anySatisfy( predicate );
    }

    @Override
    public boolean allSatisfy( LongPredicate predicate )
    {
        return delegate.allSatisfy( predicate );
    }

    @Override
    public boolean noneSatisfy( LongPredicate predicate )
    {
        return delegate.noneSatisfy( predicate );
    }

    @Override
    public long sum()
    {
        return delegate.sum();
    }

    @Override
    public long max()
    {
        return delegate.max();
    }

    @Override
    public long min()
    {
        return delegate.min();
    }

    @Override
    public LongIterator rangeIterator( int start, int stop )
    {
        return new RangeLongIterator( LongBuffer.wrap( toArray() ), start, stop );
    }

    @Override
    public RichLongSet freeze()
    {
        return new DelegatingRichLongSet( delegate.freeze() );
    }

    @Override
    public <T> T injectInto( T injectedValue,
            ObjectLongToObjectFunction<? super T,? extends T> function )
    {
        return delegate.injectInto( injectedValue, function );
    }

    @Override
    public RichIterable<LongIterable> chunk( int size )
    {
        return delegate.chunk( size );
    }

    @Override
    public MutableLongSet newEmpty()
    {
        return delegate.newEmpty();
    }

    @Override
    public long minIfEmpty( long defaultValue )
    {
        return delegate.minIfEmpty( defaultValue );
    }

    @Override
    public long maxIfEmpty( long defaultValue )
    {
        return delegate.maxIfEmpty( defaultValue );
    }

    @Override
    public double average()
    {
        return delegate.average();
    }

    @Override
    public double median()
    {
        return delegate.median();
    }

    @Override
    public long[] toSortedArray()
    {
        return delegate.toSortedArray();
    }

    @Override
    public MutableLongList toSortedList()
    {
        return delegate.toSortedList();
    }

    @Override
    public LazyLongIterable asLazy()
    {
        return delegate.asLazy();
    }

    @Override
    public MutableLongList toList()
    {
        return delegate.toList();
    }

    @Override
    public MutableLongSet toSet()
    {
        return delegate.toSet();
    }

    @Override
    public MutableLongBag toBag()
    {
        return delegate.toBag();
    }

    @Override
    public boolean containsAll( long... source )
    {
        return delegate.containsAll( source );
    }

    @Override
    public boolean containsAll( LongIterable source )
    {
        return delegate.containsAll( source );
    }

    @Override
    public <V, R extends Collection<V>> R flatCollect(
            LongToObjectFunction<? extends Iterable<V>> function,
            R target )
    {
        return delegate.flatCollect( function, target );
    }

    @Override
    public <R extends MutableBooleanCollection> R collectBoolean(
            LongToBooleanFunction function, R target )
    {
        return delegate.collectBoolean( function, target );
    }

    @Override
    public <R extends MutableByteCollection> R collectByte(
            LongToByteFunction function, R target )
    {
        return delegate.collectByte( function, target );
    }

    @Override
    public <R extends MutableCharCollection> R collectChar(
            LongToCharFunction function, R target )
    {
        return delegate.collectChar( function, target );
    }

    @Override
    public <R extends MutableShortCollection> R collectShort(
            LongToShortFunction function, R target )
    {
        return delegate.collectShort( function, target );
    }

    @Override
    public <R extends MutableIntCollection> R collectInt(
            LongToIntFunction function, R target )
    {
        return delegate.collectInt( function, target );
    }

    @Override
    public <R extends MutableFloatCollection> R collectFloat(
            LongToFloatFunction function, R target )
    {
        return delegate.collectFloat( function, target );
    }

    @Override
    public <R extends MutableLongCollection> R collectLong(
            LongToLongFunction function, R target )
    {
        return delegate.collectLong( function, target );
    }

    @Override
    public <R extends MutableDoubleCollection> R collectDouble(
            LongToDoubleFunction function, R target )
    {
        return delegate.collectDouble( function, target );
    }

    @Override
    public LongSummaryStatistics summaryStatistics()
    {
        return delegate.summaryStatistics();
    }

    @Override
    public double averageIfEmpty( double defaultValue )
    {
        return delegate.averageIfEmpty( defaultValue );
    }

    @Override
    public double medianIfEmpty( double defaultValue )
    {
        return delegate.medianIfEmpty( defaultValue );
    }

    @Override
    public boolean isEmpty()
    {
        return delegate.isEmpty();
    }

    @Override
    public boolean notEmpty()
    {
        return delegate.notEmpty();
    }

    @Override
    public String makeString()
    {
        return delegate.makeString();
    }

    @Override
    public String makeString( String separator )
    {
        return delegate.makeString( separator );
    }

    @Override
    public String makeString( String start, String separator, String end )
    {
        return delegate.makeString( start, separator, end );
    }

    @Override
    public void appendString( Appendable appendable )
    {
        delegate.appendString( appendable );
    }

    @Override
    public void appendString( Appendable appendable, String separator )
    {
        delegate.appendString( appendable, separator );
    }

    @Override
    public boolean removeIf( LongPredicate predicate )
    {
        return delegate.removeIf( predicate );
    }
}
