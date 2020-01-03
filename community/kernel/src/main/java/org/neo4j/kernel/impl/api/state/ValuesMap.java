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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.LazyIterable;
import org.eclipse.collections.api.LazyLongIterable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.api.bag.primitive.MutableBooleanBag;
import org.eclipse.collections.api.bag.primitive.MutableByteBag;
import org.eclipse.collections.api.bag.primitive.MutableCharBag;
import org.eclipse.collections.api.bag.primitive.MutableDoubleBag;
import org.eclipse.collections.api.bag.primitive.MutableFloatBag;
import org.eclipse.collections.api.bag.primitive.MutableIntBag;
import org.eclipse.collections.api.bag.primitive.MutableLongBag;
import org.eclipse.collections.api.bag.primitive.MutableShortBag;
import org.eclipse.collections.api.bag.sorted.MutableSortedBag;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.block.function.primitive.BooleanFunction;
import org.eclipse.collections.api.block.function.primitive.ByteFunction;
import org.eclipse.collections.api.block.function.primitive.CharFunction;
import org.eclipse.collections.api.block.function.primitive.DoubleFunction;
import org.eclipse.collections.api.block.function.primitive.DoubleObjectToDoubleFunction;
import org.eclipse.collections.api.block.function.primitive.FloatFunction;
import org.eclipse.collections.api.block.function.primitive.FloatObjectToFloatFunction;
import org.eclipse.collections.api.block.function.primitive.IntFunction;
import org.eclipse.collections.api.block.function.primitive.IntObjectToIntFunction;
import org.eclipse.collections.api.block.function.primitive.LongFunction;
import org.eclipse.collections.api.block.function.primitive.LongObjectToLongFunction;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ShortFunction;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.block.predicate.Predicate2;
import org.eclipse.collections.api.block.predicate.primitive.LongObjectPredicate;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.block.procedure.primitive.ObjectIntProcedure;
import org.eclipse.collections.api.collection.primitive.MutableBooleanCollection;
import org.eclipse.collections.api.collection.primitive.MutableByteCollection;
import org.eclipse.collections.api.collection.primitive.MutableCharCollection;
import org.eclipse.collections.api.collection.primitive.MutableDoubleCollection;
import org.eclipse.collections.api.collection.primitive.MutableFloatCollection;
import org.eclipse.collections.api.collection.primitive.MutableIntCollection;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.collection.primitive.MutableShortCollection;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.ImmutableLongObjectMap;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableObjectDoubleMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.map.sorted.MutableSortedMap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.api.multimap.bag.MutableBagMultimap;
import org.eclipse.collections.api.partition.bag.PartitionMutableBag;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.api.set.sorted.MutableSortedSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.api.tuple.primitive.LongObjectPair;
import org.eclipse.collections.impl.lazy.AbstractLazyIterable;
import org.eclipse.collections.impl.lazy.LazyIterableAdapter;
import org.eclipse.collections.impl.map.mutable.primitive.SynchronizedLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.UnmodifiableLongObjectMap;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ValuesMap implements MutableLongObjectMap<Value>
{
    private static final long NONE = -1L;
    private final MutableLongLongMap refs;
    private final ValuesContainer valuesContainer;

    public ValuesMap( MutableLongLongMap refs, ValuesContainer valuesContainer )
    {
        this.valuesContainer = valuesContainer;
        this.refs = refs;
    }

    @Override
    public int size()
    {
        return refs.size();
    }

    @Override
    public boolean isEmpty()
    {
        return refs.isEmpty();
    }

    @Override
    public Value getFirst()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value getLast()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains( Object object )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAllIterable( Iterable<?> source )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll( Collection<?> source )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAllArguments( Object... elements )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RichIterable<LongObjectPair<Value>> keyValuesView()
    {
        return new KeyValuesView();
    }

    @Override
    public Value put( long key, Value value )
    {
        requireNonNull( value, "Cannot put null values" );
        final Value prev = get( key );
        final long ref = valuesContainer.add( value );
        refs.put( key, ref );
        return prev;
    }

    @Override
    public void putAll( LongObjectMap<? extends Value> map )
    {
        map.forEachKeyValue( this::put );
    }

    @Override
    public Value get( long key )
    {
        final long ref = refs.getIfAbsent( key, NONE );
        return ref == NONE ? null : valuesContainer.get( ref );
    }

    @Override
    public Value getIfAbsentPut( long key, Value value )
    {
        final Value existing = get( key );
        if ( existing != null )
        {
            return existing;
        }
        put( key, value );
        return value;
    }

    @Override
    public Value getIfAbsentPut( long key, Function0<? extends Value> supplier )
    {
        final Value existing = get( key );
        if ( existing != null )
        {
            return existing;
        }
        final Value value = supplier.value();
        put( key, value );
        return value;
    }

    @Override
    public Value getIfAbsentPutWithKey( long key, LongToObjectFunction<? extends Value> function )
    {
        final Value existing = get( key );
        if ( existing != null )
        {
            return existing;
        }
        final Value value = function.valueOf( key );
        put( key, value );
        return value;
    }

    @Override
    public <P> Value getIfAbsentPutWith( long key, Function<? super P, ? extends Value> function, P parameter )
    {
        final Value existing = get( key );
        if ( existing != null )
        {
            return existing;
        }
        final Value value = function.valueOf( parameter );
        put( key, value );
        return value;
    }

    @Override
    public Value updateValue( long key, Function0<? extends Value> factory, Function<? super Value, ? extends Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> Value updateValueWith( long key, Function0<? extends Value> factory, Function2<? super Value, ? super P, ? extends Value> function, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableObjectLongMap<Value> flipUniqueValues()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableLongObjectMap<Value> tap( Procedure<? super Value> procedure )
    {
        forEachValue( procedure );
        return this;
    }

    @Override
    public void each( Procedure<? super Value> procedure )
    {
        refs.forEachKey( ref ->
        {
            final Value value = valuesContainer.get( ref );
            procedure.value( value );
        } );
    }

    @Override
    public MutableLongObjectMap<Value> select( LongObjectPredicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableLongObjectMap<Value> reject( LongObjectPredicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableLongObjectMap<Value> toImmutable()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableLongSet keySet()
    {
        return refs.keySet().asUnmodifiable();
    }

    @Override
    public LazyLongIterable keysView()
    {
        return refs.keysView();
    }

    @Override
    public ValuesMap withKeyValue( long key, Value value )
    {
        put( key, value );
        return this;
    }

    @Override
    public ValuesMap withoutKey( long key )
    {
        removeKey( key );
        return this;
    }

    @Override
    public ValuesMap withoutAllKeys( LongIterable keys )
    {
        keys.forEach( this::removeKey );
        return this;
    }

    @Override
    public MutableLongObjectMap<Value> asUnmodifiable()
    {
        return new UnmodifiableLongObjectMap<>( this );
    }

    @Override
    public MutableLongObjectMap<Value> asSynchronized()
    {
        return new SynchronizedLongObjectMap<>( this );
    }

    @Override
    public Value getIfAbsent( long key, Function0<? extends Value> ifAbsent )
    {
        final Value existing = get( key );
        if ( existing != null )
        {
            return existing;
        }
        return ifAbsent.value();
    }

    @Override
    public boolean containsKey( long key )
    {
        return refs.containsKey( key );
    }

    @Override
    public Value removeKey( long key )
    {
        final long ref = refs.removeKeyIfAbsent( key, NONE );
        return ref == NONE ? null : valuesContainer.remove( ref );
    }

    @Override
    public Value remove( long key )
    {
        return removeKey( key );
    }

    @Override
    public void clear()
    {
        refs.clear();
    }

    @Override
    public <K, VV> MutableMap<K, VV> aggregateInPlaceBy( Function<? super Value, ? extends K> groupBy, Function0<? extends VV> zeroValueFactory,
            Procedure2<? super VV, ? super Value> mutatingAggregator )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, VV> MutableMap<K, VV> aggregateBy( Function<? super Value, ? extends K> groupBy, Function0<? extends VV> zeroValueFactory,
            Function2<? super VV, ? super Value, ? extends VV> nonMutatingAggregator )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableBagMultimap<VV, Value> groupByEach( Function<? super Value, ? extends Iterable<VV>> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V, R extends MutableMultimap<V, Value>> R groupByEach( Function<? super Value, ? extends Iterable<V>> function, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableBagMultimap<VV, Value> groupBy( Function<? super Value, ? extends VV> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V, R extends MutableMultimap<V, Value>> R groupBy( Function<? super Value, ? extends V> function, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableMap<VV, Value> groupByUniqueKey( Function<? super Value, ? extends VV> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V, R extends MutableMap<V, Value>> R groupByUniqueKey( Function<? super Value, ? extends V> function, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableBag<VV> collectIf( Predicate<? super Value> predicate, Function<? super Value, ? extends VV> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V, R extends Collection<V>> R collectIf( Predicate<? super Value> predicate, Function<? super Value, ? extends V> function, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableBag<VV> collect( Function<? super Value, ? extends VV> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V, R extends Collection<V>> R collect( Function<? super Value, ? extends V> function, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableBooleanBag collectBoolean( BooleanFunction<? super Value> booleanFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends MutableBooleanCollection> R collectBoolean( BooleanFunction<? super Value> booleanFunction, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableByteBag collectByte( ByteFunction<? super Value> byteFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends MutableByteCollection> R collectByte( ByteFunction<? super Value> byteFunction, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableCharBag collectChar( CharFunction<? super Value> charFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends MutableCharCollection> R collectChar( CharFunction<? super Value> charFunction, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableDoubleBag collectDouble( DoubleFunction<? super Value> doubleFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends MutableDoubleCollection> R collectDouble( DoubleFunction<? super Value> doubleFunction, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableFloatBag collectFloat( FloatFunction<? super Value> floatFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends MutableFloatCollection> R collectFloat( FloatFunction<? super Value> floatFunction, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableIntBag collectInt( IntFunction<? super Value> intFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends MutableIntCollection> R collectInt( IntFunction<? super Value> intFunction, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableLongBag collectLong( LongFunction<? super Value> longFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends MutableLongCollection> R collectLong( LongFunction<? super Value> longFunction, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableShortBag collectShort( ShortFunction<? super Value> shortFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends MutableShortCollection> R collectShort( ShortFunction<? super Value> shortFunction, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P, VV> MutableBag<VV> collectWith( Function2<? super Value, ? super P, ? extends VV> function, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P, V, R extends Collection<V>> R collectWith( Function2<? super Value, ? super P, ? extends V> function, P parameter, R targetCollection )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableBag<VV> flatCollect( Function<? super Value, ? extends Iterable<VV>> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V, R extends Collection<V>> R flatCollect( Function<? super Value, ? extends Iterable<V>> function, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value detect( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> Value detectWith( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Value> detectOptional( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> Optional<Value> detectWithOptional( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> Value detectWithIfNone( Predicate2<? super Value, ? super P> predicate, P parameter, Function0<? extends Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int count( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> int countWith( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean anySatisfy( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> boolean anySatisfyWith( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean allSatisfy( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> boolean allSatisfyWith( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean noneSatisfy( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> boolean noneSatisfyWith( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <IV> IV injectInto( IV injectedValue, Function2<? super IV, ? super Value, ? extends IV> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int injectInto( int injectedValue, IntObjectToIntFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long injectInto( long injectedValue, LongObjectToLongFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public float injectInto( float injectedValue, FloatObjectToFloatFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double injectInto( double injectedValue, DoubleObjectToDoubleFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends Collection<Value>> R into( R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableList<Value> toList()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V extends Comparable<? super V>> MutableList<Value> toSortedListBy( Function<? super Value, ? extends V> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableSet<Value> toSet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableSortedSet<Value> toSortedSet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableSortedSet<Value> toSortedSet( Comparator<? super Value> comparator )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V extends Comparable<? super V>> MutableSortedSet<Value> toSortedSetBy( Function<? super Value, ? extends V> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableBag<Value> toBag()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableSortedBag<Value> toSortedBag()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableSortedBag<Value> toSortedBag( Comparator<? super Value> comparator )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V extends Comparable<? super V>> MutableSortedBag<Value> toSortedBagBy( Function<? super Value, ? extends V> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <NK, NV> MutableMap<NK, NV> toMap( Function<? super Value, ? extends NK> keyFunction, Function<? super Value, ? extends NV> valueFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <NK, NV> MutableSortedMap<NK, NV> toSortedMap( Function<? super Value, ? extends NK> keyFunction,
            Function<? super Value, ? extends NV> valueFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <NK, NV> MutableSortedMap<NK, NV> toSortedMap( Comparator<? super NK> comparator, Function<? super Value, ? extends NK> keyFunction,
            Function<? super Value, ? extends NV> valueFunction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public LazyIterable<Value> asLazy()
    {
        return new LazyIterableAdapter<>( this );
    }

    @Override
    public Object[] toArray()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray( T[] target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value min( Comparator<? super Value> comparator )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value max( Comparator<? super Value> comparator )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value min()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value max()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V extends Comparable<? super V>> Value minBy( Function<? super Value, ? extends V> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V extends Comparable<? super V>> Value maxBy( Function<? super Value, ? extends V> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sumOfInt( IntFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double sumOfFloat( FloatFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sumOfLong( LongFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double sumOfDouble( DoubleFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S> MutableBag<S> selectInstancesOf( Class<S> clazz )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableBag<Value> select( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends Collection<Value>> R select( Predicate<? super Value> predicate, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> MutableBag<Value> selectWith( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P, R extends Collection<Value>> R selectWith( Predicate2<? super Value, ? super P> predicate, P parameter, R targetCollection )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableBag<Value> reject( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> MutableBag<Value> rejectWith( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends Collection<Value>> R reject( Predicate<? super Value> predicate, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P, R extends Collection<Value>> R rejectWith( Predicate2<? super Value, ? super P> predicate, P parameter, R targetCollection )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionMutableBag<Value> partition( Predicate<? super Value> predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> PartitionMutableBag<Value> partitionWith( Predicate2<? super Value, ? super P> predicate, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S> MutableBag<Pair<Value, S>> zip( Iterable<S> that )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S, R extends Collection<Pair<Value, S>>> R zip( Iterable<S> that, R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableSet<Pair<Value, Integer>> zipWithIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends Collection<Pair<Value, Integer>>> R zipWithIndex( R target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RichIterable<RichIterable<Value>> chunk( int size )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableObjectLongMap<VV> sumByInt( Function<? super Value, ? extends VV> groupBy, IntFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableObjectDoubleMap<VV> sumByFloat( Function<? super Value, ? extends VV> groupBy, FloatFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableObjectLongMap<VV> sumByLong( Function<? super Value, ? extends VV> groupBy, LongFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <VV> MutableObjectDoubleMap<VV> sumByDouble( Function<? super Value, ? extends VV> groupBy, DoubleFunction<? super Value> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendString( Appendable appendable, String start, String separator, String end )
    {
        try
        {
            appendable.append( format( "ValuesMap[size: %d]", refs.size() ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void forEachKey( LongProcedure procedure )
    {
        refs.forEachKey( procedure );
    }

    @Override
    public void forEachKeyValue( LongObjectProcedure<? super Value> procedure )
    {
        refs.forEachKeyValue( ( key, ref ) ->
        {
            final Value value = valuesContainer.get( ref );
            procedure.value( key, value );
        } );
    }

    @Override
    public boolean containsValue( Object value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachValue( Procedure<? super Value> procedure )
    {
        forEachKeyValue( ( k, v ) -> procedure.value( v ) );
    }

    @Override
    public Collection<Value> values()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEach( Procedure<? super Value> procedure )
    {
        forEachValue( procedure );
    }

    @Override
    public void forEachWithIndex( ObjectIntProcedure<? super Value> objectIntProcedure )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <P> void forEachWith( Procedure2<? super Value, ? super P> procedure, P parameter )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Value> iterator()
    {
        throw new UnsupportedOperationException();
    }

    private class KeyValuesView extends AbstractLazyIterable<LongObjectPair<Value>>
    {
        @Override
        public void each( Procedure<? super LongObjectPair<Value>> procedure )
        {
            for ( LongObjectPair<Value> valueLongObjectPair : this )
            {
                procedure.value( valueLongObjectPair );
            }
        }

        @Override
        public Iterator<LongObjectPair<Value>> iterator()
        {
            Iterator<LongLongPair> refsIterator = refs.keyValuesView().iterator();
            return new Iterator<LongObjectPair<Value>>()
            {
                @Override
                public boolean hasNext()
                {
                    return refsIterator.hasNext();
                }

                @Override
                public LongObjectPair<Value> next()
                {
                    final LongLongPair key2ref = refsIterator.next();
                    final long key = key2ref.getOne();
                    final long ref = key2ref.getTwo();
                    final Value value = valuesContainer.get( ref );
                    return PrimitiveTuples.pair( key, value );
                }
            };
        }
    }
}
