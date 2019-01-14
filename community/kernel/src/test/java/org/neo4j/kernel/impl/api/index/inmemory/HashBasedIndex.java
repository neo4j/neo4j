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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.resourceIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.toPrimitiveIterator;
import static org.neo4j.internal.kernel.api.IndexQuery.IndexQueryType.exact;
import static org.neo4j.values.storable.ValueGroup.NO_VALUE;

class HashBasedIndex extends InMemoryIndexImplementation
{
    private Map<List<Object>,Set<Long>> data;

    HashBasedIndex( SchemaIndexDescriptor descriptor )
    {
        super( descriptor );
    }

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
    synchronized PrimitiveLongResourceIterator doIndexSeek( Object... propertyValues )
    {
        Set<Long> nodes = data().get( Arrays.asList( propertyValues ) );
        return asResource( nodes == null ? emptyIterator() : toPrimitiveIterator( nodes.iterator() ) );
    }

    private synchronized PrimitiveLongResourceIterator rangeSeek( Value lower, boolean includeLower, Value upper, boolean includeUpper,
            ValueGroup targetValueGroup, IndexQuery query )
    {
        Set<Long> nodeIds = new HashSet<>();
        for ( Map.Entry<List<Object>,Set<Long>> entry : data.entrySet() )
        {
            Value key = Values.of( entry.getKey().get( 0 ) );
            if ( query.acceptsValue( key ) )
            {
                nodeIds.addAll( entry.getValue() );
            }
        }
        return asResource( toPrimitiveIterator( nodeIds.iterator() ) );
    }

    private synchronized PrimitiveLongResourceIterator rangeSeekByPrefix( String prefix )
    {
        return stringSearch( entry -> entry.startsWith( prefix ) );
    }

    private synchronized PrimitiveLongResourceIterator containsString( String exactTerm )
    {
        return stringSearch( entry -> entry.contains( exactTerm ) );
    }

    private PrimitiveLongResourceIterator endsWith( String suffix )
    {
        return stringSearch( entry -> entry.endsWith( suffix ) );
    }

    private synchronized PrimitiveLongResourceIterator scan()
    {
        Iterable<Long> all = Iterables.flattenIterable( data.values() );
        return asResource( toPrimitiveIterator( all.iterator() ) );
    }

    @Override
    synchronized boolean doAdd( long nodeId, boolean applyIdempotently, Object... propertyValue )
    {
        Set<Long> nodes = data().computeIfAbsent( Arrays.asList( propertyValue ), k -> new HashSet<>() );
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
        HashBasedIndex snapshot = new HashBasedIndex( descriptor );
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
    public PrimitiveLongResourceIterator query( IndexQuery... predicates )
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
        case range:
            switch ( predicate.valueGroup().category() )
            {
            case NUMBER:
                IndexQuery.NumberRangePredicate np = (IndexQuery.NumberRangePredicate) predicate;
                return rangeSeek( np.fromValue(), np.fromInclusive(), np.toValue(), np.toInclusive(), ValueGroup.NUMBER, np );

            case TEXT:
                IndexQuery.TextRangePredicate srp = (IndexQuery.TextRangePredicate) predicate;
                return rangeSeek( srp.fromValue(), srp.fromInclusive(), srp.toValue(), srp.toInclusive(), ValueGroup.TEXT, srp );

            case TEMPORAL:
                IndexQuery.RangePredicate trp = (IndexQuery.RangePredicate) predicate;
                Value lower = trp.fromValue();
                Value upper = trp.toValue();
                return rangeSeek( lower, trp.fromInclusive(), upper, trp.toInclusive(), extractValueGroup( lower, upper ), trp );
            case GEOMETRY:
            default:
                throw new UnsupportedOperationException(
                        format( "Range scan of valueCategory %s is not supported", predicate.valueGroup().category() ) );
            }
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
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }

    @Override
    public void distinctValues( IndexProgressor.NodeValueClient client, PropertyAccessor propertyAccessor )
    {
        throw new UnsupportedOperationException();
    }

    private interface StringFilter
    {
        boolean test( String s );
    }

    private PrimitiveLongResourceIterator stringSearch( StringFilter filter )
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
        return asResource( toPrimitiveIterator( nodeIds.iterator() ) );
    }

    private PrimitiveLongResourceIterator asResource( PrimitiveLongIterator iterator )
    {
        return resourceIterator( iterator, null );
    }

    @Override
    boolean hasSameContentsAs( InMemoryIndexImplementation other )
    {
        return this.data.equals( ((HashBasedIndex)other).data );
    }

    private ValueGroup extractValueGroup( Value o1, Value o2 )
    {
        if ( o1.valueGroup() != NO_VALUE && o2.valueGroup() != NO_VALUE )
        {
            assert o1.valueGroup() == o2.valueGroup() : "o1.valueGroup=" + o1.valueGroup() + ", o2.valueGroup=" + o2.valueGroup();
            return o1.valueGroup();
        }
        else if ( o1.valueGroup() != NO_VALUE )
        {
            return o1.valueGroup();
        }
        else if ( o2.valueGroup() != NO_VALUE )
        {
            return o2.valueGroup();
        }
        throw new IllegalArgumentException( "Can't decide ValueGroup from null values" );
    }
}
