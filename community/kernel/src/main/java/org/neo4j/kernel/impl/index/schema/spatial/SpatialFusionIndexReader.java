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
package org.neo4j.kernel.impl.index.schema.spatial;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.impl.index.schema.spatial.SpatialFusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class SpatialFusionIndexReader implements IndexReader
{
    private final Map<CoordinateReferenceSystem,IndexReader> readerMap;
    private final Selector selector;
    private final int[] propertyKeys;

    SpatialFusionIndexReader( Map<CoordinateReferenceSystem,IndexReader> readerMap, Selector selector, int[] propertyKeys )
    {
        this.readerMap = readerMap;
        this.selector = selector;
        this.propertyKeys = propertyKeys;
    }

    @Override
    public void close()
    {
        forAll( ( reader ) -> ((IndexReader) reader).close(), readerMap.values().toArray() );
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        return selector.select( readerMap, propertyValues ).countIndexedNodes( nodeId, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        return new SpatialFusionIndexSampler( readerMap.values().stream().map( IndexReader::createSampler ).toArray( IndexSampler[]::new ) );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        if ( predicates.length > 1 )
        {
            throw new IndexNotApplicableKernelException( "Spatial index doesn't handle composite queries" );
        }

        if ( predicates[0] instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicates[0];
            CoordinateReferenceSystem crs = ((PointValue) exactPredicate.value()).getCoordinateReferenceSystem();
            return readerMap.get( crs ).query( predicates );
        }

        if ( predicates[0] instanceof GeometryRangePredicate )
        {
            CoordinateReferenceSystem crs = ((GeometryRangePredicate) predicates[0]).from().getCoordinateReferenceSystem();
            return readerMap.get( crs ).query( predicates[0] );
        }

        // todo: There will be no ordering of the node ids here. Is this a problem?
        if ( predicates[0] instanceof ExistsPredicate )
        {
            List<PrimitiveLongResourceIterator> iterators = new ArrayList<>();
            for (IndexReader reader : readerMap.values())
            {
                iterators.add( reader.query( predicates[0] ) );
            }
            return PrimitiveLongResourceCollections.concat( iterators );
        }

        // TODO fix fix fix
        throw new IndexNotApplicableKernelException( "Spatial index can't handle the given query: " + predicates[0] );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, IndexQuery... predicates )
            throws IndexNotApplicableKernelException
    {
        if ( predicates.length > 1 )
        {
            throw new IndexNotApplicableKernelException( "Spatial index doesn't handle composite queries" );
        }

        if ( predicates[0] instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicates[0];
            CoordinateReferenceSystem crs = ((PointValue) exactPredicate.value()).getCoordinateReferenceSystem();
            readerMap.get( crs ).query( cursor, indexOrder, predicates );
            return;
        }

        if ( predicates[0] instanceof GeometryRangePredicate )
        {
            CoordinateReferenceSystem crs = ((GeometryRangePredicate) predicates[0]).from().getCoordinateReferenceSystem();
            readerMap.get( crs ).query( cursor, indexOrder, predicates[0] );
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
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, propertyKeys );
            cursor.initialize( multiProgressor, propertyKeys );
            for (IndexReader reader : readerMap.values())
            {
                reader.query( multiProgressor, indexOrder, predicates[0] );
            }
            return;
        }

        // TODO fallback?? fix fix fix
        throw new IndexNotApplicableKernelException( "Spatial index can't handle the given query: " + predicates[0] );
    }

    @Override
    public boolean hasFullNumberPrecision( IndexQuery... predicates )
    {
        // TODO is this ok?
        return false;
    }

    /**
     * Combine multiple progressor to act like one single logical progressor seen from clients perspective.
     */
    private class BridgingIndexProgressor implements IndexProgressor.NodeValueClient, IndexProgressor
    {
        private final NodeValueClient client;
        private final int[] keys;
        private final Queue<IndexProgressor> progressors;
        private IndexProgressor current;

        BridgingIndexProgressor( NodeValueClient client, int[] keys )
        {
            this.client = client;
            this.keys = keys;
            progressors = new ArrayDeque<>();
        }

        @Override
        public boolean next()
        {
            if ( current == null )
            {
                current = progressors.poll();
            }
            while ( current != null )
            {
                if ( current.next() )
                {
                    return true;
                }
                else
                {
                    current.close();
                    current = progressors.poll();
                }
            }
            return false;
        }

        @Override
        public void close()
        {
            progressors.forEach( IndexProgressor::close );
        }

        @Override
        public void initialize( IndexProgressor progressor, int[] keys )
        {
            assertKeysAlign( keys );
            progressors.add( progressor );
        }

        private void assertKeysAlign( int[] keys )
        {
            for ( int i = 0; i < this.keys.length; i++ )
            {
                if ( this.keys[i] != keys[i] )
                {
                    throw new UnsupportedOperationException( "Can not chain multiple progressors with different key set." );
                }
            }
        }

        @Override
        public boolean acceptNode( long reference, Value[] values )
        {
            return client.acceptNode( reference, values );
        }
    }
}
