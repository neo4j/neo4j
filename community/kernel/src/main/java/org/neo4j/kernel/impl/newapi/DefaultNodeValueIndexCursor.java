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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueGroup;

import static java.util.Arrays.stream;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.asSet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

final class DefaultNodeValueIndexCursor extends IndexCursor<IndexProgressor>
        implements NodeValueIndexCursor, NodeValueClient
{
    private Read read;
    private long node;
    private IndexQuery[] query;
    private Value[] values;
    private PrimitiveLongIterator added = emptyIterator();
    private PrimitiveLongSet removed = emptySet();
    private boolean needsValues;
    private final DefaultCursors pool;

    DefaultNodeValueIndexCursor( DefaultCursors pool )
    {
        this.pool = pool;
        node = NO_ID;
    }

    @Override
    public void initialize( SchemaIndexDescriptor descriptor, IndexProgressor progressor,
                            IndexQuery[] query )
    {
        assert query != null;
        super.initialize( progressor );
        this.query = query;

        if ( query.length > 0 )
        {
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
        else
        {
            // this is used for distinct values query
            needsValues = true;
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
        if ( added.hasNext() )
        {
            this.node = added.next();
            this.values = null;
            return true;
        }
        else
        {
            return innerNext();
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
        if ( !isClosed() )
        {
            super.close();
            this.node = NO_ID;
            this.query = null;
            this.values = null;
            this.read = null;
            this.added = emptyIterator();
            this.removed = PrimitiveLongCollections.emptySet();

            pool.accept( this );
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
                   ", underlying record=" + super.toString() + " ]";
        }
    }

    private void prefixQuery( SchemaIndexDescriptor descriptor, IndexQuery.StringPrefixPredicate predicate )
    {
        needsValues = true;
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();
            PrimitiveLongReadableDiffSets changes = txState
                    .indexUpdatesForRangeSeekByPrefix( descriptor, predicate.prefix() );
            added = changes.augment( emptyIterator() );
            removed = removed( txState, changes );
        }
    }

    private void rangeQuery( SchemaIndexDescriptor descriptor, IndexQuery.RangePredicate<?> predicate )
    {
        ValueGroup valueGroup = predicate.valueGroup();
        ValueCategory category = valueGroup.category();
        this.needsValues = category == ValueCategory.TEXT || category == ValueCategory.NUMBER || category == ValueCategory.TEMPORAL;
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();
            PrimitiveLongReadableDiffSets changes = txState.indexUpdatesForRangeSeek( descriptor, predicate );
            added = changes.augment( emptyIterator() );
            removed = removed( txState, changes );
        }
    }

    private void scanQuery( SchemaIndexDescriptor descriptor )
    {
        needsValues = true;
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();
            PrimitiveLongReadableDiffSets changes = txState.indexUpdatesForScan( descriptor );
            added = changes.augment( emptyIterator() );
            removed = removed( txState, changes );
        }
    }

    private void suffixOrContainsQuery( SchemaIndexDescriptor descriptor, IndexQuery query )
    {
        needsValues = true;
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();
            PrimitiveLongReadableDiffSets changes = txState.indexUpdatesForSuffixOrContains( descriptor, query );
            added = changes.augment( emptyIterator() );
            removed = removed( txState, changes );
        }
    }

    private void seekQuery( SchemaIndexDescriptor descriptor, IndexQuery[] query )
    {
        needsValues = false;
        IndexQuery.ExactPredicate[] exactPreds = assertOnlyExactPredicates( query );
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();
            PrimitiveLongReadableDiffSets changes = read.txState()
                    .indexUpdatesForSeek( descriptor, IndexQuery.asValueTuple( exactPreds ) );
            added = changes.augment( emptyIterator() );
            removed = removed( txState, changes );
        }
    }

    private PrimitiveLongSet removed( TransactionState txState, PrimitiveLongReadableDiffSets changes )
    {
        PrimitiveLongSet longSet = asSet( txState.addedAndRemovedNodes().getRemoved() );
        longSet.addAll( changes.getRemoved().iterator() );
        return longSet;
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
