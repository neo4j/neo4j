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
package org.neo4j.kernel.impl.util.collection;

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
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.SetIterable;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;

import java.util.Collection;
import java.util.LongSummaryStatistics;

public abstract class BaseRichLongSet implements RichLongSet
{
    protected final LongSet longSet;

    protected BaseRichLongSet( LongSet longSet )
    {
        this.longSet = longSet;
    }

    @Override
    public LongSet tap( LongProcedure procedure )
    {
        return longSet.tap( procedure );
    }

    @Override
    public boolean equals( Object o )
    {
        return longSet.equals( o );
    }

    @Override
    public int hashCode()
    {
        return longSet.hashCode();
    }

    @Override
    public LongSet select( LongPredicate predicate )
    {
        return longSet.select( predicate );
    }

    @Override
    public LongSet reject( LongPredicate predicate )
    {
        return longSet.reject( predicate );
    }

    @Override
    public <V> SetIterable<V> collect(
            LongToObjectFunction<? extends V> function )
    {
        return longSet.collect( function );
    }

    @Override
    public ImmutableLongSet toImmutable()
    {
        return longSet.toImmutable();
    }

    @Override
    public LongIterator longIterator()
    {
        return longSet.longIterator();
    }

    @Override
    public long[] toArray()
    {
        return longSet.toArray();
    }

    @Override
    public boolean contains( long value )
    {
        return longSet.contains( value );
    }

    @Override
    public boolean containsAll( long... source )
    {
        return longSet.containsAll( source );
    }

    @Override
    public boolean containsAll( LongIterable source )
    {
        return longSet.containsAll( source );
    }

    @Override
    public void forEach( LongProcedure procedure )
    {
        longSet.forEach( procedure );
    }

    @Override
    public void each( LongProcedure procedure )
    {
        longSet.each( procedure );
    }

    @Override
    public <R extends MutableLongCollection> R select(
            LongPredicate predicate, R target )
    {
        return longSet.select( predicate, target );
    }

    @Override
    public <R extends MutableLongCollection> R reject(
            LongPredicate predicate, R target )
    {
        return longSet.reject( predicate, target );
    }

    @Override
    public <V, R extends Collection<V>> R collect(
            LongToObjectFunction<? extends V> function, R target )
    {
        return longSet.collect( function, target );
    }

    @Override
    public <V, R extends Collection<V>> R flatCollect(
            LongToObjectFunction<? extends Iterable<V>> function,
            R target )
    {
        return longSet.flatCollect( function, target );
    }

    @Override
    public <R extends MutableBooleanCollection> R collectBoolean(
            LongToBooleanFunction function, R target )
    {
        return longSet.collectBoolean( function, target );
    }

    @Override
    public <R extends MutableByteCollection> R collectByte(
            LongToByteFunction function, R target )
    {
        return longSet.collectByte( function, target );
    }

    @Override
    public <R extends MutableCharCollection> R collectChar(
            LongToCharFunction function, R target )
    {
        return longSet.collectChar( function, target );
    }

    @Override
    public <R extends MutableShortCollection> R collectShort(
            LongToShortFunction function, R target )
    {
        return longSet.collectShort( function, target );
    }

    @Override
    public <R extends MutableIntCollection> R collectInt(
            LongToIntFunction function, R target )
    {
        return longSet.collectInt( function, target );
    }

    @Override
    public <R extends MutableFloatCollection> R collectFloat(
            LongToFloatFunction function, R target )
    {
        return longSet.collectFloat( function, target );
    }

    @Override
    public <R extends MutableLongCollection> R collectLong(
            LongToLongFunction function, R target )
    {
        return longSet.collectLong( function, target );
    }

    @Override
    public <R extends MutableDoubleCollection> R collectDouble(
            LongToDoubleFunction function, R target )
    {
        return longSet.collectDouble( function, target );
    }

    @Override
    public long detectIfNone( LongPredicate predicate, long ifNone )
    {
        return longSet.detectIfNone( predicate, ifNone );
    }

    @Override
    public int count( LongPredicate predicate )
    {
        return longSet.count( predicate );
    }

    @Override
    public boolean anySatisfy( LongPredicate predicate )
    {
        return longSet.anySatisfy( predicate );
    }

    @Override
    public boolean allSatisfy( LongPredicate predicate )
    {
        return longSet.allSatisfy( predicate );
    }

    @Override
    public boolean noneSatisfy( LongPredicate predicate )
    {
        return longSet.noneSatisfy( predicate );
    }

    @Override
    public MutableLongList toList()
    {
        return longSet.toList();
    }

    @Override
    public MutableLongSet toSet()
    {
        return longSet.toSet();
    }

    @Override
    public MutableLongBag toBag()
    {
        return longSet.toBag();
    }

    @Override
    public LazyLongIterable asLazy()
    {
        return longSet.asLazy();
    }

    @Override
    public <T> T injectInto( T injectedValue,
            ObjectLongToObjectFunction<? super T,? extends T> function )
    {
        return longSet.injectInto( injectedValue, function );
    }

    @Override
    public RichIterable<LongIterable> chunk( int size )
    {
        return longSet.chunk( size );
    }

    @Override
    public long sum()
    {
        return longSet.sum();
    }

    @Override
    public LongSummaryStatistics summaryStatistics()
    {
        return longSet.summaryStatistics();
    }

    @Override
    public long max()
    {
        return longSet.max();
    }

    @Override
    public long maxIfEmpty( long defaultValue )
    {
        return longSet.maxIfEmpty( defaultValue );
    }

    @Override
    public long min()
    {
        return longSet.min();
    }

    @Override
    public long minIfEmpty( long defaultValue )
    {
        return longSet.minIfEmpty( defaultValue );
    }

    @Override
    public double average()
    {
        return longSet.average();
    }

    @Override
    public double averageIfEmpty( double defaultValue )
    {
        return longSet.averageIfEmpty( defaultValue );
    }

    @Override
    public double median()
    {
        return longSet.median();
    }

    @Override
    public double medianIfEmpty( double defaultValue )
    {
        return longSet.medianIfEmpty( defaultValue );
    }

    @Override
    public long[] toSortedArray()
    {
        return longSet.toSortedArray();
    }

    @Override
    public MutableLongList toSortedList()
    {
        return longSet.toSortedList();
    }

    @Override
    public int size()
    {
        return longSet.size();
    }

    @Override
    public boolean isEmpty()
    {
        return longSet.isEmpty();
    }

    @Override
    public boolean notEmpty()
    {
        return longSet.notEmpty();
    }

    @Override
    public String toString()
    {
        return longSet.toString();
    }

    @Override
    public String makeString()
    {
        return longSet.makeString();
    }

    @Override
    public String makeString( String separator )
    {
        return longSet.makeString( separator );
    }

    @Override
    public String makeString( String start, String separator, String end )
    {
        return longSet.makeString( start, separator, end );
    }

    @Override
    public void appendString( Appendable appendable )
    {
        longSet.appendString( appendable );
    }

    @Override
    public void appendString( Appendable appendable, String separator )
    {
        longSet.appendString( appendable, separator );
    }

    @Override
    public void appendString( Appendable appendable, String start, String separator, String end )
    {
        longSet.appendString( appendable, start, separator, end );
    }
}
