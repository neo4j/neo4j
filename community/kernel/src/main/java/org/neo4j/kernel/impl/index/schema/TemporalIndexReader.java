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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.index.schema.fusion.BridgingIndexProgressor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class TemporalIndexReader extends TemporalIndexCache<TemporalIndexPartReader<?>,IOException> implements IndexReader
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
        if ( predicates[0] instanceof ExistsPredicate )
        {
            return PrimitiveLongResourceCollections.concat(
                    Iterables.map( partReader -> partReader.query( predicates ), this ) );
        }
        else
        {
            NodeValueIterator nodeValueIterator = new NodeValueIterator();
            query( nodeValueIterator, IndexOrder.NONE, predicates );
            return nodeValueIterator;
        }
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, IndexQuery... predicates )
    {
        if ( predicates[0] instanceof ExistsPredicate )
        {
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
            cursor.initialize( descriptor, multiProgressor, predicates );
            for ( NativeSchemaIndexReader reader : this )
            {
                reader.query( multiProgressor, indexOrder, predicates );
            }
        }
        else
        {
            if ( validPredicates( predicates ) )
            {
                NativeSchemaIndexReader<?,NativeSchemaValue> part = uncheckedSelect( predicates[0].valueGroup() );
                if ( part != null )
                {
                    part.query( cursor, indexOrder, predicates );
                }
            }
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return true;
    }

    private boolean validPredicates( IndexQuery[] predicates )
    {
        return predicates[0] instanceof ExactPredicate || predicates[0] instanceof GeometryRangePredicate;
    }

    /**
     * To create TemporalIndexPartReaders on demand, the PartFactory maintains a reference to the parent TemporalIndexAccessor.
     * The creation of a part reader can then be delegated to the correct PartAccessor.
     */
    static class PartFactory implements TemporalIndexCache.Factory<TemporalIndexPartReader<?>, IOException>
    {
        private final TemporalIndexAccessor accessor;

        PartFactory( TemporalIndexAccessor accessor )
        {
            this.accessor = accessor;
        }

        @Override
        public TemporalIndexPartReader<?> newDate() throws IOException
        {
            return accessor.selectOrElse( ValueGroup.DATE, TemporalIndexAccessor.PartAccessor::newReader, null );
        }

        @Override
        public TemporalIndexPartReader<?> newDateTime()
        {
            throw new UnsupportedOperationException( "Illiterate" );
        }

        @Override
        public TemporalIndexPartReader<?> newDateTimeZoned()
        {
            throw new UnsupportedOperationException( "Illiterate" );
        }

        @Override
        public TemporalIndexPartReader<?> newTime()
        {
            return accessor.selectOrElse( ValueGroup.LOCAL_TIME, TemporalIndexAccessor.PartAccessor::newReader, null );
        }

        @Override
        public TemporalIndexPartReader<?> newTimeZoned()
        {
            throw new UnsupportedOperationException( "Illiterate" );
        }

        @Override
        public TemporalIndexPartReader<?> newDuration()
        {
            throw new UnsupportedOperationException( "Illiterate" );
        }
    }
}
