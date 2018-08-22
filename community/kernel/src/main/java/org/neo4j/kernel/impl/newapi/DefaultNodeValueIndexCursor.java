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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueGroup;

import static java.util.Arrays.stream;
import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

final class DefaultNodeValueIndexCursor extends IndexCursor<IndexProgressor>
        implements NodeValueIndexCursor, NodeValueClient
{
    private Read read;
    private Resource resource;
    private long node;
    private IndexQuery[] query;
    private Value[] values;
    private LongIterator added = ImmutableEmptyLongIterator.INSTANCE;
    private Iterator<NodeWithPropertyValues> addedWithValues = Collections.emptyIterator();
    private LongSet removed = LongSets.immutable.empty();
    private boolean needsValues;
    private final DefaultCursors pool;

    DefaultNodeValueIndexCursor( DefaultCursors pool )
    {
        this.pool = pool;
        node = NO_ID;
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, boolean needsValues )
    {
        assert query != null && query.length > 0;
        super.initialize( progressor );

        this.needsValues = needsValues;
        this.query = query;

        IndexQuery firstPredicate = query[0];
        switch ( firstPredicate.type() )
        {
        case exact:
            seekQuery( descriptor, query );
            break;

        case exists:
            scanQuery( descriptor );
            break;

        case range:
            assert query.length == 1;
            rangeQuery( descriptor, (IndexQuery.RangePredicate) firstPredicate );
            break;

        case stringPrefix:
            assert query.length == 1;
            prefixQuery( descriptor, (IndexQuery.StringPrefixPredicate) firstPredicate );
            break;

        case stringSuffix:
        case stringContains:
            assert query.length == 1;
            suffixOrContainsQuery( descriptor, firstPredicate );
            break;

        default:
            throw new UnsupportedOperationException( "Query not supported: " + Arrays.toString( query ) );
        }
    }

    private boolean isRemoved( long reference )
    {
        return removed.contains( reference );
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
    public boolean needsValues()
    {
        return needsValues;
    }

    @Override
    public boolean next()
    {
        if ( !needsValues && added.hasNext() )
        {
            this.node = added.next();
            this.values = null;
            return true;
        }
        else if ( needsValues && addedWithValues.hasNext() )
        {
            NodeWithPropertyValues nodeWithPropertyValues = addedWithValues.next();
            this.node = nodeWithPropertyValues.getNodeId();
            this.values = nodeWithPropertyValues.getValues();
            return true;
        }
        else
        {
            return innerNext();
        }
    }

    public void setRead( Read read, Resource resource )
    {
        this.read = read;
        this.resource = resource;
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
        if ( !isClosed() )
        {
            super.close();
            this.node = NO_ID;
            this.query = null;
            this.values = null;
            this.read = null;
            this.added = ImmutableEmptyLongIterator.INSTANCE;
            this.addedWithValues = Collections.emptyIterator();
            this.removed = LongSets.immutable.empty();

            try
            {
                if ( resource != null )
                {
                    resource.close();
                    resource = null;
                }
            }
            finally
            {
                pool.accept( this );
            }
        }
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
            String keys = query == null ? "unknown" : Arrays.toString( stream( query ).map( IndexQuery::propertyKeyId ).toArray( Integer[]::new ) );
            return "NodeValueIndexCursor[node=" + node + ", open state with: keys=" + keys +
                   ", values=" + Arrays.toString( values ) +
                   ", underlying record=" + super.toString() + "]";
        }
    }

    private void prefixQuery( IndexDescriptor descriptor, IndexQuery.StringPrefixPredicate predicate )
    {
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();

            if ( needsValues )
            {
                DiffSets<NodeWithPropertyValues> changes =
                        TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix( txState, descriptor, predicate.prefix() );
                addedWithValues = changes.getAdded().iterator();
                removed = removed( txState, changes.getRemoved() );
            }
            else
            {
                LongDiffSets changes = TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix( txState, descriptor, predicate.prefix() );
                added = changes.augment( ImmutableEmptyLongIterator.INSTANCE );
                removed = removed( txState, changes );
            }
        }
    }

    private void rangeQuery( IndexDescriptor descriptor, IndexQuery.RangePredicate<?> predicate )
    {
        ValueGroup valueGroup = predicate.valueGroup();
        ValueCategory category = valueGroup.category();
        // We have values for text, number, and temporal but nor for other categories, so even if a NodeValueClientValue
        // wants values, we can't provide them
        this.needsValues = this.needsValues && (category == ValueCategory.TEXT || category == ValueCategory.NUMBER || category == ValueCategory.TEMPORAL);

        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();

            if ( needsValues )
            {
                DiffSets<NodeWithPropertyValues> changes = TxStateIndexChanges.indexUpdatesWithValuesForRangeSeek( txState, descriptor, predicate );
                addedWithValues = changes.getAdded().iterator();
                removed = removed( txState, changes.getRemoved() );
            }
            else
            {
                LongDiffSets changes = TxStateIndexChanges.indexUpdatesForRangeSeek( txState, descriptor, predicate );
                added = changes.augment( ImmutableEmptyLongIterator.INSTANCE );
                removed = removed( txState, changes );
            }
        }
    }

    private void scanQuery( IndexDescriptor descriptor )
    {
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();

            if ( needsValues )
            {
                DiffSets<NodeWithPropertyValues> changes =
                        TxStateIndexChanges.indexUpdatesWithValuesForScan( txState, descriptor );
                addedWithValues = changes.getAdded().iterator();
                removed = removed( txState, changes.getRemoved() );
            }
            else
            {
                LongDiffSets changes = TxStateIndexChanges.indexUpdatesForScan( txState, descriptor );
                added = changes.augment( ImmutableEmptyLongIterator.INSTANCE );
                removed = removed( txState, changes );
            }
        }
    }

    private void suffixOrContainsQuery( IndexDescriptor descriptor, IndexQuery query )
    {
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();

            if ( needsValues )
            {
                DiffSets<NodeWithPropertyValues> changes =
                        TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains( txState, descriptor, query );
                addedWithValues = changes.getAdded().iterator();
                removed = removed( txState, changes.getRemoved() );
            }
            else
            {
                LongDiffSets changes = TxStateIndexChanges.indexUpdatesForSuffixOrContains( txState, descriptor, query );
                added = changes.augment( ImmutableEmptyLongIterator.INSTANCE );
                removed = removed( txState, changes );
            }
        }
    }

    private void seekQuery( IndexDescriptor descriptor, IndexQuery[] query )
    {
        // We should never be asked to provide values for a seek query. But a NodeValueClientFilter
        // might ask us, and then we just have to ignore that and set it back to false
        this.needsValues = false;

        IndexQuery.ExactPredicate[] exactPreds = assertOnlyExactPredicates( query );
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();

            LongDiffSets changes = TxStateIndexChanges.indexUpdatesForSeek( txState, descriptor, IndexQuery.asValueTuple( exactPreds ) );
            added = changes.augment( ImmutableEmptyLongIterator.INSTANCE );
            removed = removed( txState, changes );
        }
    }

    private LongSet removed( TransactionState txState, LongDiffSets changes )
    {
        return mergeToSet( txState.addedAndRemovedNodes().getRemoved(), changes.getRemoved() );
    }

    private LongSet removed( TransactionState txState, Set<NodeWithPropertyValues> removed )
    {
        LongSet removedFromTXState = txState.addedAndRemovedNodes().getRemoved();
        final MutableLongSet set = new LongHashSet( removedFromTXState.size() + removed.size() );
        set.addAll( removedFromTXState );
        for ( NodeWithPropertyValues nodeWithPropertyValues : removed )
        {
            set.add( nodeWithPropertyValues.getNodeId() );
        }
        return set;
    }

    private static IndexQuery.ExactPredicate[] assertOnlyExactPredicates( IndexQuery[] predicates )
    {
        IndexQuery.ExactPredicate[] exactPredicates;
        if ( predicates.getClass() == IndexQuery.ExactPredicate[].class )
        {
            exactPredicates = (IndexQuery.ExactPredicate[]) predicates;
        }
        else
        {
            exactPredicates = new IndexQuery.ExactPredicate[predicates.length];
            for ( int i = 0; i < predicates.length; i++ )
            {
                if ( predicates[i] instanceof IndexQuery.ExactPredicate )
                {
                    exactPredicates[i] = (IndexQuery.ExactPredicate) predicates[i];
                }
                else
                {
                    // TODO: what to throw?
                    throw new IllegalArgumentException( "Query not supported: " + Arrays.toString( predicates ) );
                }
            }
        }
        return exactPredicates;
    }

    public void release()
    {
        // nothing to do
    }
}
