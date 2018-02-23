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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.NumberRangePredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.schema.BridgingIndexProgressor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class FusionIndexReader implements IndexReader
{
    private final IndexReader nativeReader;
    private final IndexReader spatialReader;
    private final IndexReader luceneReader;
    private final Selector selector;
    private final IndexDescriptor descriptor;

    FusionIndexReader( IndexReader nativeReader, IndexReader spatialReader, IndexReader luceneReader, Selector selector,
            IndexDescriptor descriptor )
    {
        this.nativeReader = nativeReader;
        this.spatialReader = spatialReader;
        this.luceneReader = luceneReader;
        this.selector = selector;
        this.descriptor = descriptor;
    }

    @Override
    public void close()
    {
        forAll( Resource::close, nativeReader, spatialReader, luceneReader );
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        return selector.select( nativeReader, spatialReader, luceneReader, propertyValues ).countIndexedNodes( nodeId, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        return new FusionIndexSampler( nativeReader.createSampler(), spatialReader.createSampler(), luceneReader.createSampler() );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        if ( predicates.length > 1 )
        {
            return luceneReader.query( predicates );
        }

        if ( predicates[0] instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicates[0];
            return selector.select( nativeReader, spatialReader, luceneReader, exactPredicate.value() ).query( predicates );
        }

        if ( predicates[0] instanceof NumberRangePredicate )
        {
            return nativeReader.query( predicates[0] );
        }

        if ( predicates[0] instanceof GeometryRangePredicate )
        {
            return spatialReader.query( predicates[0] );
        }

        // todo: There will be no ordering of the node ids here. Is this a problem?
        if ( predicates[0] instanceof ExistsPredicate )
        {
            PrimitiveLongResourceIterator nativeResult = nativeReader.query( predicates[0] );
            PrimitiveLongResourceIterator spatialResult = spatialReader.query( predicates[0] );
            PrimitiveLongResourceIterator luceneResult = luceneReader.query( predicates[0] );
            return PrimitiveLongResourceCollections.concat( nativeResult, spatialResult, luceneResult );
        }

        return luceneReader.query( predicates );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, IndexQuery... predicates )
            throws IndexNotApplicableKernelException
    {
        if ( predicates.length > 1 )
        {
            luceneReader.query( cursor, indexOrder, predicates );
            return;
        }

        if ( predicates[0] instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicates[0];
            selector.select( nativeReader, spatialReader, luceneReader, exactPredicate.value() ).query( cursor, indexOrder, predicates );
            return;
        }

        if ( predicates[0] instanceof NumberRangePredicate )
        {
            nativeReader.query( cursor, indexOrder, predicates[0] );
            return;
        }

        if ( predicates[0] instanceof GeometryRangePredicate )
        {
            spatialReader.query( cursor, indexOrder, predicates[0] );
            return;
        }

        // todo: There will be no ordering of the node ids here. Is this a problem?
        if ( predicates[0] instanceof ExistsPredicate )
        {
            if ( indexOrder != IndexOrder.NONE )
            {
                throw new UnsupportedOperationException(
                        format( "Tried to query index with unsupported order %s. Supported orders for query %s are %s.",
                                indexOrder, Arrays.toString( predicates ), IndexOrder.NONE ) );
            }
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor,
                    descriptor.schema().getPropertyIds() );
            cursor.initialize( descriptor, multiProgressor, predicates );
            nativeReader.query( multiProgressor, indexOrder, predicates[0] );
            spatialReader.query( multiProgressor, indexOrder, predicates[0] );
            luceneReader.query( multiProgressor, indexOrder, predicates[0] );
            return;
        }

        luceneReader.query( cursor, indexOrder, predicates );
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        if ( predicates.length > 1 )
        {
            return false;
        }

        IndexQuery predicate = predicates[0];
        if ( predicate instanceof ExactPredicate )
        {
            Value value = ((ExactPredicate) predicate).value();
            return selector.select(
                    nativeReader.hasFullValuePrecision( predicates ),
                    spatialReader.hasFullValuePrecision( predicates ),
                    luceneReader.hasFullValuePrecision( predicates ), value );
        }
        return predicates[0] instanceof NumberRangePredicate && nativeReader.hasFullValuePrecision( predicates );
    }
}
