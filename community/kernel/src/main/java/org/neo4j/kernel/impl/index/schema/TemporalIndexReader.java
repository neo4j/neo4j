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
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class TemporalIndexReader extends TemporalIndexCache<TemporalIndexPartReader<?>> implements IndexReader
{
    private final SchemaIndexDescriptor descriptor;

    TemporalIndexReader( SchemaIndexDescriptor descriptor, TemporalIndexAccessor accessor )
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
        NativeSchemaIndexReader<?,NativeSchemaValue> partReader = uncheckedSelect( propertyValues[0].valueGroup() );
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
            throw new IllegalArgumentException( "Only single property temporal indexes are supported." );
        }
        IndexQuery predicate = predicates[0];
        if ( predicate instanceof ExistsPredicate )
        {
            loadAll();
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
            cursor.initialize( descriptor, multiProgressor, predicates );
            for ( NativeSchemaIndexReader<?,NativeSchemaValue> reader : this )
            {
                reader.query( multiProgressor, indexOrder, predicates );
            }
        }
        else
        {
            if ( validPredicate( predicate ) )
            {
                NativeSchemaIndexReader<?,NativeSchemaValue> part = uncheckedSelect( predicate.valueGroup() );
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
        return true;
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
