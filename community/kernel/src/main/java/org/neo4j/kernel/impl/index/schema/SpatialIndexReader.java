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
package org.neo4j.kernel.impl.index.schema;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.kernel.impl.api.schema.BridgingIndexProgressor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class SpatialIndexReader extends SpatialIndexCache<SpatialIndexPartReader<NativeIndexValue>> implements IndexReader
{
    private final IndexDescriptor descriptor;

    SpatialIndexReader( IndexDescriptor descriptor, SpatialIndexAccessor accessor )
    {
        super( new PartFactory( accessor ) );
        this.descriptor = descriptor;
    }

    @Override
    public void close()
    {
        forAll( Resource::close, this );
    }

    @Override
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        NativeIndexReader<SpatialIndexKey,NativeIndexValue> partReader =
                uncheckedSelect( ((PointValue) propertyValues[0]).getCoordinateReferenceSystem() );
        return partReader == null ? 0L : partReader.countIndexedNodes( nodeId, propertyKeyIds, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        List<IndexSampler> samplers = new ArrayList<>();
        for ( SpatialIndexPartReader<NativeIndexValue> partReader : this )
        {
            samplers.add( partReader.createSampler() );
        }
        return new FusionIndexSampler( samplers );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates )
    {
        NodeValueIterator nodeValueIterator = new NodeValueIterator();
        query( nodeValueIterator, IndexOrder.NONE, nodeValueIterator.needsValues(), predicates );
        return nodeValueIterator;
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, boolean needsValues, IndexQuery... predicates )
    {
        // Spatial does not support providing values
        if ( needsValues )
        {
            throw new IllegalStateException( "Spatial index does not support providing values" );
        }

        if ( predicates.length != 1 )
        {
            throw new IllegalArgumentException( "Only single property spatial indexes are supported." );
        }
        IndexQuery predicate = predicates[0];
        if ( predicate instanceof ExistsPredicate )
        {
            loadAll();
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
            cursor.initialize( descriptor, multiProgressor, predicates, indexOrder, false );
            for ( NativeIndexReader<SpatialIndexKey,NativeIndexValue> reader : this )
            {
                reader.query( multiProgressor, indexOrder, false, predicates );
            }
        }
        else
        {
            if ( validPredicate( predicate ) )
            {
                CoordinateReferenceSystem crs;
                if ( predicate instanceof IndexQuery.ExactPredicate )
                {
                    crs = ((PointValue) ((IndexQuery.ExactPredicate) predicate).value()).getCoordinateReferenceSystem();
                }
                else if ( predicate instanceof IndexQuery.GeometryRangePredicate )
                {
                    crs = ((IndexQuery.GeometryRangePredicate) predicate).crs();
                }
                else
                {
                    throw new IllegalArgumentException( "Wrong type of predicate, couldn't get CoordinateReferenceSystem" );
                }
                SpatialIndexPartReader<NativeIndexValue> part = uncheckedSelect( crs );
                if ( part != null )
                {
                    part.query( cursor, indexOrder, false, predicates );
                }
                else
                {
                    cursor.initialize( descriptor, IndexProgressor.EMPTY, predicates, indexOrder, false );
                }
            }
            else
            {
                cursor.initialize( descriptor, IndexProgressor.EMPTY, predicates, indexOrder, false );
            }
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }

    @Override
    public void distinctValues( IndexProgressor.NodeValueClient cursor, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        loadAll();
        BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
        cursor.initialize( descriptor, multiProgressor, new IndexQuery[0], IndexOrder.NONE, false );
        for ( NativeIndexReader<?,NativeIndexValue> reader : this )
        {
            reader.distinctValues( multiProgressor, propertyAccessor, needsValues );
        }
    }

    private boolean validPredicate( IndexQuery predicate )
    {
        return predicate instanceof IndexQuery.ExactPredicate || predicate instanceof IndexQuery.RangePredicate;
    }

    /**
     * To create TemporalIndexPartReaders on demand, the PartFactory maintains a reference to the parent TemporalIndexAccessor.
     * The creation of a part reader can then be delegated to the correct PartAccessor.
     */
    static class PartFactory implements Factory<SpatialIndexPartReader<NativeIndexValue>>
    {
        private final SpatialIndexAccessor accessor;

        PartFactory( SpatialIndexAccessor accessor )
        {
            this.accessor = accessor;
        }

        @Override
        public SpatialIndexPartReader<NativeIndexValue> newSpatial( CoordinateReferenceSystem crs )
        {
            return accessor.selectOrElse( crs, SpatialIndexAccessor.PartAccessor::newReader, null );
        }
    }
}
