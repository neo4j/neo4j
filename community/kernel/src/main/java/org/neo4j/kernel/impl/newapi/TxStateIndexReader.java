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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.values.storable.Value;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.resourceIterator;
import static org.neo4j.kernel.impl.api.StateHandlingStatementOperations.assertOnlyExactPredicates;

/**
 * An index reader that decorate the {@link #query(IndexQuery...)} method
 * with data from the transaction state.
 */
class TxStateIndexReader implements IndexReader
{
    private final IndexReader indexReader;
    private final TxStateHolder txStateHolder;
    private final IndexDescriptor descriptor;

    TxStateIndexReader( IndexReader indexReader, TxStateHolder txStateHolder, IndexDescriptor descriptor )
    {
        this.indexReader = indexReader;
        this.txStateHolder = txStateHolder;
        this.descriptor = descriptor;
    }

    @Override
    public void close()
    {
        indexReader.close();
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        return indexReader.countIndexedNodes( nodeId, propertyValues );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient client, IndexOrder indexOrder, IndexQuery... query )
            throws IndexNotApplicableKernelException
    {
        indexReader.query( client, indexOrder, query );
    }

    @Override
    public IndexSampler createSampler()
    {
        return indexReader.createSampler();
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        assert predicates.length >= 1;

        PrimitiveLongResourceIterator src = indexReader.query( predicates );
        IndexQuery firstPredicate = predicates[0];
        switch ( firstPredicate.type() )
        {
        case exact:
            return exactIterator( src, predicates );

        case stringSuffix:
        case stringContains:
        case exists:
            return scanIterator( src );

        case rangeNumeric:
            assert predicates.length == 1;
            return numericRangeIterator( (IndexQuery.NumberRangePredicate) firstPredicate, src);

        case rangeString:
            assert predicates.length == 1;
            return stringRangeIterator( (IndexQuery.StringRangePredicate) firstPredicate, src );

        case stringPrefix:
            assert predicates.length == 1;
            return stringPrefixIterator( (IndexQuery.StringPrefixPredicate) firstPredicate, src );

        default:
            throw new UnsupportedOperationException( "Query not supported: " + Arrays.toString( predicates ) );
        }
    }

    private PrimitiveLongResourceIterator stringPrefixIterator( IndexQuery.StringPrefixPredicate predicate,
            PrimitiveLongResourceIterator src )
    {
        if ( txStateHolder.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChangesForPrefix =
                    txStateHolder.txState().indexUpdatesForRangeSeekByPrefix( descriptor, predicate.prefix() );
            ReadableDiffSets<Long> nodes = txStateHolder.txState().addedAndRemovedNodes();

            // Apply to actual index lookup
            return resourceIterator( nodes.augmentWithRemovals( labelPropertyChangesForPrefix.augment( src ) ),
                    src );
        }
        return src;
    }

    private PrimitiveLongResourceIterator stringRangeIterator( IndexQuery.StringRangePredicate predicate,
            PrimitiveLongResourceIterator src )
    {
        if ( txStateHolder.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChangesForNumber =
                    txStateHolder.txState().indexUpdatesForRangeSeekByString(
                            descriptor, predicate.from(), predicate.fromInclusive(), predicate.to(),
                            predicate.toInclusive() );
            ReadableDiffSets<Long> nodes = txStateHolder.txState().addedAndRemovedNodes();

            return resourceIterator( nodes.augmentWithRemovals( labelPropertyChangesForNumber.augment( src ) ),
                    src );
        }
        return src;
    }

    private PrimitiveLongResourceIterator numericRangeIterator( IndexQuery.NumberRangePredicate predicate,
            PrimitiveLongResourceIterator src )
    {
        if ( txStateHolder.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChangesForNumber =
                    txStateHolder.txState().indexUpdatesForRangeSeekByNumber(
                            descriptor, predicate.from(), predicate.fromInclusive(), predicate.to(),
                            predicate.toInclusive() );
            ReadableDiffSets<Long> nodes = txStateHolder.txState().addedAndRemovedNodes();

            return resourceIterator( nodes.augmentWithRemovals( labelPropertyChangesForNumber.augment( src ) ),
                    src );
        }
        return src;
    }

    private PrimitiveLongResourceIterator scanIterator( PrimitiveLongResourceIterator src )
    {
        if ( txStateHolder.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChanges =
                    txStateHolder.txState().indexUpdatesForScan( descriptor );
            ReadableDiffSets<Long> nodes = txStateHolder.txState().addedAndRemovedNodes();

            return resourceIterator( nodes.augmentWithRemovals( labelPropertyChanges.augment( src ) ), src );
        }
        return src;
    }

    private PrimitiveLongResourceIterator exactIterator( PrimitiveLongResourceIterator src, IndexQuery[] predicates )
    {
        IndexQuery.ExactPredicate[] exactPreds = assertOnlyExactPredicates( predicates );
        if ( txStateHolder.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChanges =
                    txStateHolder.txState()
                            .indexUpdatesForSeek( descriptor, IndexQuery.asValueTuple( exactPreds ) );
            ReadableDiffSets<Long> nodes = txStateHolder.txState().addedAndRemovedNodes();

            PrimitiveLongIterator primitiveLongIterator =
                    nodes.augmentWithRemovals( labelPropertyChanges.augment( src ) );
            // Apply to actual index lookup
            return resourceIterator( primitiveLongIterator, src );
        }
        return src;
    }

    @Override
    public boolean hasFullNumberPrecision( IndexQuery... predicates )
    {
        return indexReader.hasFullNumberPrecision( predicates );
    }
}
