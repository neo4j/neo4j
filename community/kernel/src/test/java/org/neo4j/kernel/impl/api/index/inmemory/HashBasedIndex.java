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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.toPrimitiveIterator;
import static org.neo4j.kernel.api.schema.IndexQuery.IndexQueryType.exact;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.COMPARE_VALUES;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.SuperType.NUMBER;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.SuperType.STRING;

class HashBasedIndex extends InMemoryIndexImplementation
{
    private Map<List<Object>,Set<Long>> data;

    public Map<List<Object>,Set<Long>> data()
    {
        if ( data == null )
        {
            throw new IllegalStateException( "Index has not been created, or has been dropped." );
        }
        return data;
    }

    @Override
    public synchronized String toString()
    {
        return getClass().getSimpleName() + data;
    }

    @Override
    synchronized void initialize()
    {
        data = new HashMap<>();
    }

    @Override
    synchronized void drop()
    {
        data = null;
    }

    @Override
    synchronized PrimitiveLongIterator doIndexSeek( Object... propertyValues )
    {
        Set<Long> nodes = data().get( Arrays.asList( propertyValues ) );
        return nodes == null ? PrimitiveLongCollections.emptyIterator() : toPrimitiveIterator( nodes.iterator() );
    }

    private synchronized PrimitiveLongIterator rangeSeekByNumberInclusive( Number lower, Number upper )
    {
        Set<Long> nodeIds = new HashSet<>();
        for ( Map.Entry<List<Object>,Set<Long>> entry : data.entrySet() )
        {
            Object key = entry.getKey().get( 0 );
            if ( NUMBER.isSuperTypeOf( key ) )
            {
                boolean lowerFilter = lower == null || COMPARE_VALUES.compare( key, lower ) >= 0;
                boolean upperFilter = upper == null || COMPARE_VALUES.compare( key, upper ) <= 0;

                if ( lowerFilter && upperFilter )
                {
                    nodeIds.addAll( entry.getValue() );
                }
            }
        }
        return toPrimitiveIterator( nodeIds.iterator() );
    }

    private synchronized PrimitiveLongIterator rangeSeekByString( String lower, boolean includeLower, String upper,
            boolean includeUpper )
    {
        Set<Long> nodeIds = new HashSet<>();
        for ( Map.Entry<List<Object>,Set<Long>> entry : data.entrySet() )
        {
            Object key = entry.getKey().get( 0 );
            if ( STRING.isSuperTypeOf( key ) )
            {
                boolean lowerFilter;
                boolean upperFilter;

                if ( lower == null )
                {
                    lowerFilter = true;
                }
                else
                {
                    int cmp = COMPARE_VALUES.compare( key, lower );
                    lowerFilter = (includeLower && cmp >= 0) || (cmp > 0);
                }

                if ( upper == null )
                {
                    upperFilter = true;
                }
                else
                {
                    int cmp = COMPARE_VALUES.compare( key, upper );
                    upperFilter = (includeUpper && cmp <= 0) || (cmp < 0);
                }

                if ( lowerFilter && upperFilter )
                {
                    nodeIds.addAll( entry.getValue() );
                }
            }
        }
        return toPrimitiveIterator( nodeIds.iterator() );
    }

    private synchronized PrimitiveLongIterator rangeSeekByPrefix( String prefix )
    {
        return stringSearch( ( String entry ) -> entry.startsWith( prefix ) );
    }

    private synchronized PrimitiveLongIterator containsString( String exactTerm )
    {
        return stringSearch( ( String entry ) -> entry.contains( exactTerm ) );
    }

    private PrimitiveLongIterator endsWith( String suffix )
    {
        return stringSearch( ( String entry ) -> entry.endsWith( suffix ) );
    }

    private synchronized PrimitiveLongIterator scan()
    {
        Iterable<Long> all = Iterables.flattenIterable( data.values() );
        return toPrimitiveIterator( all.iterator() );
    }

    @Override
    synchronized boolean doAdd( long nodeId, boolean applyIdempotently, Object... propertyValue )
    {
        Set<Long> nodes = data().get( Arrays.asList( propertyValue ) );
        if ( nodes == null )
        {
            data().put( Arrays.asList( propertyValue ), nodes = new HashSet<>() );
        }
        // In this implementation we don't care about idempotency.
        return nodes.add( nodeId );
    }

    @Override
    synchronized void doRemove( long nodeId, Object... propertyValues )
    {
        Set<Long> nodes = data().get( Arrays.asList( propertyValues ) );
        if ( nodes != null )
        {
            nodes.remove( nodeId );
        }
    }

    @Override
    synchronized void remove( long nodeId )
    {
        for ( Set<Long> nodes : data().values() )
        {
            nodes.remove( nodeId );
        }
    }

    @Override
    synchronized void iterateAll( IndexEntryIterator iterator ) throws Exception
    {
        for ( Map.Entry<List<Object>,Set<Long>> entry : data().entrySet() )
        {
            iterator.visitEntry( entry.getKey(), entry.getValue() );
        }
    }

    @Override
    public synchronized long maxCount()
    {
        return ids().size();
    }

    @Override
    public synchronized Iterator<Long> iterator()
    {
        return ids().iterator();
    }

    private Collection<Long> ids()
    {
        Set<Long> allIds = new HashSet<>();
        for ( Set<Long> someIds : data().values() )
        {
            allIds.addAll( someIds );
        }
        return allIds;
    }

    @Override
    synchronized InMemoryIndexImplementation snapshot()
    {
        HashBasedIndex snapshot = new HashBasedIndex();
        snapshot.initialize();
        for ( Map.Entry<List<Object>,Set<Long>> entry : data().entrySet() )
        {
            snapshot.data().put( entry.getKey(), new HashSet<>( entry.getValue() ) );
        }
        return snapshot;
    }

    @Override
    protected synchronized long doCountIndexedNodes( long nodeId, Object... propertyValues )
    {
        Set<Long> candidates = data().get( Arrays.asList( propertyValues ) );
        return candidates != null && candidates.contains( nodeId ) ? 1 : 0;
    }

    @Override
    public synchronized IndexSampler createSampler()
    {
        return new HashBasedIndexSampler( data );
    }

    @Override
    public PrimitiveLongIterator query( IndexQuery... predicates )
    {
        if ( predicates.length > 1 )
        {
            Value[] values = new Value[predicates.length];
            for ( int i = 0; i < predicates.length; i++ )
            {
                assert predicates[i].type() == exact : "composite indexes only supported for seek";
                values[i] = ((IndexQuery.ExactPredicate)predicates[i]).value();
            }
            return seek( values );
        }
        assert predicates.length == 1 : "composite indexes not yet supported, except for seek on all properties";
        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exists:
            return scan();
        case exact:
            return seek( ((IndexQuery.ExactPredicate) predicate).value() );
        case rangeNumeric:
            IndexQuery.NumberRangePredicate np = (IndexQuery.NumberRangePredicate) predicate;
            return rangeSeekByNumberInclusive( np.from(), np.to() );
        case rangeString:
            IndexQuery.StringRangePredicate srp = (IndexQuery.StringRangePredicate) predicate;
            return rangeSeekByString( srp.from(), srp.fromInclusive(), srp.to(), srp.toInclusive() );
        case stringPrefix:
            IndexQuery.StringPrefixPredicate spp = (IndexQuery.StringPrefixPredicate) predicate;
            return rangeSeekByPrefix( spp.prefix() );
        case stringContains:
            IndexQuery.StringContainsPredicate scp = (IndexQuery.StringContainsPredicate) predicate;
            return containsString( scp.contains() );
        case stringSuffix:
            IndexQuery.StringSuffixPredicate ssp = (IndexQuery.StringSuffixPredicate) predicate;
            return endsWith( ssp.suffix() );
        default:
            throw new RuntimeException( "Unsupported query: " + Arrays.toString( predicates ) );
        }
    }

    @Override
    public boolean hasFullNumberPrecision( IndexQuery... predicates )
    {
        return false;
    }

    private interface StringFilter
    {
        boolean test( String s );
    }

    private PrimitiveLongIterator stringSearch( StringFilter filter )
    {
        Set<Long> nodeIds = new HashSet<>();
        for ( Map.Entry<List<Object>,Set<Long>> entry : data.entrySet() )
        {
            Object key = entry.getKey().get( 0 );
            if ( key instanceof String )
            {
                if ( filter.test( (String) key ) )
                {
                    nodeIds.addAll( entry.getValue() );
                }
            }
        }
        return toPrimitiveIterator( nodeIds.iterator() );
    }

}
