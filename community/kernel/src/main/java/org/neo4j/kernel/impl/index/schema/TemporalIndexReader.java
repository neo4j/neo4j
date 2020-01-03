/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class TemporalIndexReader extends TemporalIndexCache<TemporalIndexPartReader<?>> implements IndexReader
{
    private final IndexDescriptor descriptor;

    TemporalIndexReader( IndexDescriptor descriptor, TemporalIndexAccessor accessor )
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
        NativeIndexReader<?,NativeIndexValue> partReader = uncheckedSelect( propertyValues[0].valueGroup() );
        return partReader == null ? 0L : partReader.countIndexedNodes( nodeId, propertyKeyIds, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        List<IndexSampler> samplers = new ArrayList<>();
        for ( TemporalIndexPartReader<?> partReader : this )
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
        if ( predicates.length != 1 )
        {
            throw new IllegalArgumentException( "Only single property temporal indexes are supported." );
        }
        IndexQuery predicate = predicates[0];
        if ( predicate instanceof ExistsPredicate )
        {
            loadAll();
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
            cursor.initialize( descriptor, multiProgressor, predicates, indexOrder, needsValues );
            for ( NativeIndexReader<?,NativeIndexValue> reader : this )
            {
                reader.query( multiProgressor, indexOrder, needsValues, predicates );
            }
        }
        else
        {
            if ( validPredicate( predicate ) )
            {
                NativeIndexReader<?,NativeIndexValue> part = uncheckedSelect( predicate.valueGroup() );
                if ( part != null )
                {
                    part.query( cursor, indexOrder, needsValues, predicates );
                }
                else
                {
                    cursor.initialize( descriptor, IndexProgressor.EMPTY, predicates, indexOrder, needsValues );
                }
            }
            else
            {
                cursor.initialize( descriptor, IndexProgressor.EMPTY, predicates, indexOrder, needsValues );
            }
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return true;
    }

    @Override
    public void distinctValues( IndexProgressor.NodeValueClient cursor, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        loadAll();
        BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
        cursor.initialize( descriptor, multiProgressor, new IndexQuery[0], IndexOrder.NONE, needsValues );
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
    static class PartFactory implements TemporalIndexCache.Factory<TemporalIndexPartReader<?>>
    {
        private final TemporalIndexAccessor accessor;

        PartFactory( TemporalIndexAccessor accessor )
        {
            this.accessor = accessor;
        }

        @Override
        public TemporalIndexPartReader<?> newDate()
        {
            return accessor.selectOrElse( ValueGroup.DATE, TemporalIndexAccessor.PartAccessor::newReader, null );
        }

        @Override
        public TemporalIndexPartReader<?> newLocalDateTime()
        {
            return accessor.selectOrElse( ValueGroup.LOCAL_DATE_TIME, TemporalIndexAccessor.PartAccessor::newReader, null );
        }

        @Override
        public TemporalIndexPartReader<?> newZonedDateTime()
        {
            return accessor.selectOrElse( ValueGroup.ZONED_DATE_TIME, TemporalIndexAccessor.PartAccessor::newReader, null );
        }

        @Override
        public TemporalIndexPartReader<?> newLocalTime()
        {
            return accessor.selectOrElse( ValueGroup.LOCAL_TIME, TemporalIndexAccessor.PartAccessor::newReader, null );
        }

        @Override
        public TemporalIndexPartReader<?> newZonedTime()
        {
            return accessor.selectOrElse( ValueGroup.ZONED_TIME, TemporalIndexAccessor.PartAccessor::newReader, null );
        }

        @Override
        public TemporalIndexPartReader<?> newDuration()
        {
            return accessor.selectOrElse( ValueGroup.DURATION, TemporalIndexAccessor.PartAccessor::newReader, null );
        }
    }
}
