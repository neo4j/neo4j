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
import org.neo4j.internal.kernel.api.IndexQuery.StringRangePredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.schema.BridgingIndexProgressor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.Selector;

import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.internal.kernel.api.IndexQuery.StringContainsPredicate;
import static org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import static org.neo4j.internal.kernel.api.IndexQuery.StringSuffixPredicate;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class FusionIndexReader implements IndexReader
{
    private final IndexReader stringReader;
    private final IndexReader numberReader;
    private final IndexReader spatialReader;
    private final IndexReader temporalReader;
    private final IndexReader luceneReader;
    private final IndexReader[] readers;
    private final Selector selector;
    private final SchemaIndexDescriptor descriptor;

    FusionIndexReader(
            IndexReader stringReader,
            IndexReader numberReader,
            IndexReader spatialReader,
            IndexReader temporalReader,
            IndexReader luceneReader,
            Selector selector,
            SchemaIndexDescriptor descriptor )
    {
        this.stringReader = stringReader;
        this.numberReader = numberReader;
        this.spatialReader = spatialReader;
        this.temporalReader = temporalReader;
        this.luceneReader = luceneReader;
        this.readers = array( stringReader, numberReader, spatialReader, temporalReader, luceneReader );
        this.selector = selector;
        this.descriptor = descriptor;
    }

    @Override
    public void close()
    {
        forAll( Resource::close, readers );
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        return selector.select( stringReader, numberReader, spatialReader, temporalReader, luceneReader, propertyValues )
                .countIndexedNodes( nodeId, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        return new FusionIndexSampler(
                stringReader.createSampler(),
                numberReader.createSampler(),
                spatialReader.createSampler(),
                temporalReader.createSampler(),
                luceneReader.createSampler() );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        if ( predicates.length > 1 )
        {
            return luceneReader.query( predicates );
        }
        IndexQuery predicate = predicates[0];

        if ( predicate instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicates[0];
            return selector.select( stringReader, numberReader, spatialReader, temporalReader, luceneReader, exactPredicate.value() ).query( predicates );
        }

        if ( predicate instanceof NumberRangePredicate )
        {
            return numberReader.query( predicates );
        }

        if ( predicate instanceof StringRangePredicate )
        {
            return stringReader.query( predicate );
        }

        if ( predicate instanceof GeometryRangePredicate )
        {
            return spatialReader.query( predicates );
        }

// TODO: support temporal range queries
//        if ( predicates[0] instanceof TemporalRangePredicate )
//        {
//            return temporalReader.query( predicates[0] );
//        }

        // todo: There will be no ordering of the node ids here. Is this a problem?
        if ( predicate instanceof ExistsPredicate )
        {
            PrimitiveLongResourceIterator[] iterators = new PrimitiveLongResourceIterator[readers.length];
            for ( int i = 0; i < readers.length; i++ )
            {
                iterators[i] = readers[i].query( predicates );
            }
            return PrimitiveLongResourceCollections.concat( iterators );
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
        IndexQuery predicate = predicates[0];

        if ( predicate instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicates[0];
            selector.select( stringReader, numberReader, spatialReader, temporalReader, luceneReader, exactPredicate.value() )
                    .query( cursor, indexOrder, predicate );
            return;
        }

        if ( predicate instanceof NumberRangePredicate )
        {
            numberReader.query( cursor, indexOrder, predicate );
            return;
        }

        if ( predicate instanceof StringRangePredicate ||
             predicate instanceof StringPrefixPredicate ||
             predicate instanceof StringSuffixPredicate ||
             predicate instanceof StringContainsPredicate )
        {
            stringReader.query( cursor, indexOrder, predicate );
            return;
        }

        if ( predicate instanceof GeometryRangePredicate )
        {
            spatialReader.query( cursor, indexOrder, predicate );
            return;
        }

// TODO: support temporal range queries
//        if ( predicates[0] instanceof TemporalRangePredicate )
//        {
//            return temporalReader.query( predicates[0] );
//        }

        // todo: There will be no ordering of the node ids here. Is this a problem?
        if ( predicate instanceof ExistsPredicate )
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
            for ( IndexReader reader : readers )
            {
                reader.query( multiProgressor, indexOrder, predicate );
            }
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
                    stringReader.hasFullValuePrecision( predicates ),
                    numberReader.hasFullValuePrecision( predicates ),
                    spatialReader.hasFullValuePrecision( predicates ),
                    temporalReader.hasFullValuePrecision( predicates ),
                    luceneReader.hasFullValuePrecision( predicates ), value );
        }
        if ( predicate instanceof NumberRangePredicate )
        {
            return numberReader.hasFullValuePrecision( predicates );
        }

// TODO: support temporal range queries
//        if ( predicate instanceof temporalRangePredicate )
//        {
//            return temporalReader.hasFullValuePrecision( predicates );
//        }
        return false;
    }
}
