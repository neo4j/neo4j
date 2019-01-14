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

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.index.schema.fusion.BridgingIndexProgressor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class SpatialIndexReader extends SpatialIndexCache<SpatialIndexPartReader<NativeSchemaValue>> implements IndexReader
{
    private final SchemaIndexDescriptor descriptor;

    SpatialIndexReader( SchemaIndexDescriptor descriptor, SpatialIndexAccessor accessor )
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
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        NativeSchemaIndexReader<SpatialSchemaKey,NativeSchemaValue> partReader =
                uncheckedSelect( ((PointValue) propertyValues[0]).getCoordinateReferenceSystem() );
        return partReader == null ? 0L : partReader.countIndexedNodes( nodeId, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        return new FusionIndexSampler( Iterators.stream( iterator() ).map( IndexReader::createSampler ).toArray( IndexSampler[]::new ) );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates )
    {
        NodeValueIterator nodeValueIterator = new NodeValueIterator();
        query( nodeValueIterator, IndexOrder.NONE, predicates );
        return nodeValueIterator;
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, IndexQuery... predicates )
    {
        if ( predicates.length != 1 )
        {
            throw new IllegalArgumentException( "Only single property spatial indexes are supported." );
        }
        IndexQuery predicate = predicates[0];
        if ( predicate instanceof ExistsPredicate )
        {
            loadAll();
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
            cursor.initialize( descriptor, multiProgressor, predicates );
            for ( NativeSchemaIndexReader<SpatialSchemaKey,NativeSchemaValue> reader : this )
            {
                reader.query( multiProgressor, indexOrder, predicates );
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
                SpatialIndexPartReader<NativeSchemaValue> part = uncheckedSelect( crs );
                if ( part != null )
                {
                    part.query( cursor, indexOrder, predicates );
                }
                else
                {
                    cursor.initialize( descriptor, IndexProgressor.EMPTY, predicates );
                }
            }
            else
            {
                cursor.initialize( descriptor, IndexProgressor.EMPTY, predicates );
            }
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }

    @Override
    public void distinctValues( IndexProgressor.NodeValueClient cursor, PropertyAccessor propertyAccessor )
    {
        loadAll();
        BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
        cursor.initialize( descriptor, multiProgressor, new IndexQuery[0] );
        for ( NativeSchemaIndexReader<?,NativeSchemaValue> reader : this )
        {
            reader.distinctValues( multiProgressor, propertyAccessor );
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
    static class PartFactory implements Factory<SpatialIndexPartReader<NativeSchemaValue>>
    {
        private final SpatialIndexAccessor accessor;

        PartFactory( SpatialIndexAccessor accessor )
        {
            this.accessor = accessor;
        }

        @Override
        public SpatialIndexPartReader<NativeSchemaValue> newSpatial( CoordinateReferenceSystem crs )
        {
            return accessor.selectOrElse( crs, SpatialIndexAccessor.PartAccessor::newReader, null );
        }
    }
}
