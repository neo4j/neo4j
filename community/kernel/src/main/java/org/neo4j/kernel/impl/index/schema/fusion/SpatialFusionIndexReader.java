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

import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class SpatialFusionIndexReader implements IndexReader
{
    private final Map<CoordinateReferenceSystem,IndexReader> readerMap;
    private final SchemaIndexDescriptor descriptor;

    SpatialFusionIndexReader( Map<CoordinateReferenceSystem,IndexReader> readerMap, SchemaIndexDescriptor descriptor )
    {
        this.readerMap = readerMap;
        this.descriptor = descriptor;
    }

    private <T> T select( Map<CoordinateReferenceSystem,T> instances, Value... values )
    {
        assert values.length == 1;
        PointValue pointValue = (PointValue) values[0];
        return instances.get( pointValue.getCoordinateReferenceSystem() );
    }

    @Override
    public void close()
    {
        forAll( Resource::close, readerMap.values() );
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        Long ans = selectAndRun( reader -> reader.countIndexedNodes( nodeId, propertyValues ), propertyValues );
        return ans == null ? 0L : ans;
    }

    interface ActionableWithResult<R>
    {
        R doIt( IndexReader reader );
    }

    private <R> R selectAndRun( ActionableWithResult<R> actionable, Value... values )
    {
        IndexReader reader = select( readerMap, values );
        if ( reader != null )
        {
            return actionable.doIt( reader );
        }
        return null;
    }

    @Override
    public IndexSampler createSampler()
    {
        return new FusionIndexSampler( readerMap.values().stream().map( IndexReader::createSampler ).toArray( IndexSampler[]::new ) );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        NodeValueIterator nodeValueIterator = new NodeValueIterator();
        query( nodeValueIterator, IndexOrder.NONE, predicates );
        return nodeValueIterator;
    }

    private IndexReader selectIf( IndexQuery... predicates )
    {
        if ( predicates[0] instanceof ExactPredicate )
        {
            return readerMap.get( ((PointValue) ((ExactPredicate) predicates[0]).value()).getCoordinateReferenceSystem() );
        }
        else if ( predicates[0] instanceof GeometryRangePredicate )
        {
            return readerMap.get( ((GeometryRangePredicate) predicates[0]).crs() );
        }
        return null;
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        if ( predicates[0] instanceof ExistsPredicate )
        {
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
            cursor.initialize( descriptor, multiProgressor, predicates );
            for ( IndexReader reader : readerMap.values() )
            {
                reader.query( multiProgressor, indexOrder, predicates[0] );
            }
        }
        else
        {
            IndexReader reader = selectIf( predicates );
            if ( reader != null )
            {
                reader.query( cursor, indexOrder, predicates );
            }
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }
}
