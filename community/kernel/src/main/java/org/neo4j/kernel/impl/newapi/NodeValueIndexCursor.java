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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.values.storable.Value;

import static java.util.Arrays.stream;
import static org.neo4j.kernel.impl.api.StateHandlingStatementOperations.assertOnlyExactPredicates;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class NodeValueIndexCursor extends IndexCursor
        implements org.neo4j.internal.kernel.api.NodeValueIndexCursor, NodeValueClient
{
    private Read read;
    private long node;
    private IndexQuery[] query;
    private Value[] values;
    private PrimitiveLongIterator added;
    private Set<Long> removed;

    NodeValueIndexCursor()
    {
        node = NO_ID;
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor,
            IndexQuery[] query )
    {
        assert query != null && query.length > 0;
        super.initialize( progressor );

        IndexQuery firstPredicate = query[0];
        switch ( firstPredicate.type() )
        {
        case exact:
            seekQuery( descriptor, query );
            break;

        case stringSuffix:
        case stringContains:
        case exists:
            scanQuery( descriptor );
            break;

        case rangeNumeric:
            assert query.length == 1;
            numericRangeQuery( descriptor, (IndexQuery.NumberRangePredicate) query[0] );
        break;

        case rangeString:
            assert query.length == 1;
            stringRangeQuery( descriptor, (IndexQuery.StringRangePredicate) query[0] );
        break;

        case stringPrefix:
            prefixQuery( descriptor, (IndexQuery.StringPrefixPredicate) query[0] );
            break;

        default:
            throw new UnsupportedOperationException( "Query not supported: " + Arrays.toString( query ) );
        }
    }

    private boolean isRemoved( long reference )
    {
        return removed != null && removed.contains( reference );
    }

    @Override
    public boolean acceptNode( long reference, Value[] values )
    {
        if ( isRemoved( reference ) )
        {
            return false;
        }
        else
        {
            this.node = reference;
            this.values = values;
            return true;
        }
    }

    @Override
    public boolean next()
    {
        if ( added != null && added.hasNext() )
        {
            this.node = added.next();
            this.values = null;
            return true;
        }
        else
        {
            return super.next();
        }
    }

    public void setRead( Read read )
    {
        this.read = read;
    }

    @Override
    public void node( NodeCursor cursor )
    {
        read.singleNode( node, cursor );
    }

    @Override
    public long nodeReference()
    {
        return node;
    }

    @Override
    public int numberOfProperties()
    {
        return query == null ? 0 : query.length;
    }

    @Override
    public int propertyKey( int offset )
    {
        return query[offset].propertyKeyId();
    }

    @Override
    public boolean hasValue()
    {
        return values != null;
    }

    @Override
    public Value propertyValue( int offset )
    {
        return values[offset];
    }

    @Override
    public void close()
    {
        super.close();
        this.node = NO_ID;
        this.query = null;
        this.values = null;
        this.read = null;
        this.added = null;
        this.removed = null;
    }

    @Override
    public boolean isClosed()
    {
        return super.isClosed();
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "NodeValueIndexCursor[closed state]";
        }
        else
        {
            return "NodeValueIndexCursor[node=" + node + ", open state with: keys=" +
                   Arrays.toString( stream( query ).map( IndexQuery::propertyKeyId ).toArray( Integer[]::new ) ) +
                   ", values=" + Arrays.toString( values ) +
                   ", underlying record=" + super.toString() + " ]";
        }
    }

    private void prefixQuery( IndexDescriptor descriptor, IndexQuery.StringPrefixPredicate predicate )
    {
        if ( read.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> changes =
                    read.txState().indexUpdatesForRangeSeekByPrefix( descriptor, predicate.prefix() );
            added =
                    changes
                            .augment( PrimitiveLongCollections.emptyIterator() );
            removed = new HashSet<>( read.txState().addedAndRemovedNodes().getRemoved() );
            removed.addAll( changes.getRemoved() );
        }
    }

    private void stringRangeQuery( IndexDescriptor descriptor, IndexQuery.StringRangePredicate predicate )
    {
        if ( read.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> changes = read.txState().indexUpdatesForRangeSeekByString(
                    descriptor, predicate.from(), predicate.fromInclusive(), predicate.to(),
                    predicate.toInclusive() );
            added =
                    changes.augment( PrimitiveLongCollections.emptyIterator() );
            removed = new HashSet<>( read.txState().addedAndRemovedNodes().getRemoved() );
            removed.addAll( changes.getRemoved() );
        }
    }

    private void numericRangeQuery( IndexDescriptor descriptor, IndexQuery.NumberRangePredicate predicate )
    {
        if ( read.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> changes = read.txState().indexUpdatesForRangeSeekByNumber(
                    descriptor, predicate.from(), predicate.fromInclusive(), predicate.to(),
                    predicate.toInclusive() );
            added =
                    changes.augment( PrimitiveLongCollections.emptyIterator() );
            removed = new HashSet<>( read.txState().addedAndRemovedNodes().getRemoved() );
            removed.addAll( changes.getRemoved() );
        }
    }

    private void scanQuery( IndexDescriptor descriptor )
    {
        if ( read.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> changes = read.txState().indexUpdatesForScan( descriptor );
            added = changes
                    .augment( PrimitiveLongCollections.emptyIterator() );
            removed = new HashSet<>( read.txState().addedAndRemovedNodes().getRemoved() );
            removed.addAll( changes.getRemoved() );
        }
    }

    private void seekQuery( IndexDescriptor descriptor, IndexQuery[] query )
    {
        IndexQuery.ExactPredicate[] exactPreds = assertOnlyExactPredicates( query );
        if ( read.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> changes = read.txState()
                    .indexUpdatesForSeek( descriptor, IndexQuery.asValueTuple( exactPreds ) );
            added = changes
                    .augment( PrimitiveLongCollections.emptyIterator() );
            removed = new HashSet<>( read.txState().addedAndRemovedNodes().getRemoved() );
            removed.addAll( changes.getRemoved() );
        }
    }
}
