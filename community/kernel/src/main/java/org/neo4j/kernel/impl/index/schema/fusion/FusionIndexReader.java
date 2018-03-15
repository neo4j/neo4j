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
import org.neo4j.internal.kernel.api.IndexQuery.RangePredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.schema.BridgingIndexProgressor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;

class FusionIndexReader extends FusionIndexBase<IndexReader> implements IndexReader
{
    private final SchemaIndexDescriptor descriptor;

    FusionIndexReader( IndexReader[] readers, Selector selector, SchemaIndexDescriptor descriptor )
    {
        super( readers, selector );
        this.descriptor = descriptor;
    }

    @Override
    public void close()
    {
        forAll( Resource::close, instances );
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        return selector.select( instances, propertyValues ).countIndexedNodes( nodeId, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        return new FusionIndexSampler( instancesAs( IndexSampler.class, IndexReader::createSampler ) );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        IndexReader instance = selector.select( instances, predicates );
        if ( instance != null )
        {
            return instance.query( predicates );
        }
        else
        {
            PrimitiveLongResourceIterator[] converted = instancesAs( PrimitiveLongResourceIterator.class, reader -> reader.query( predicates ) );
            return PrimitiveLongResourceCollections.concat( converted );
        }
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, IndexQuery... predicates )
            throws IndexNotApplicableKernelException
    {
        IndexReader instance = selector.select( instances, predicates );
        if ( instance != null )
        {
            instance.query( cursor, indexOrder, predicates );
        }
        else
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
            for ( IndexReader reader : instances )
            {
                reader.query( multiProgressor, indexOrder, predicates );
            }
        }
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
            return selector.select( instances, value ).hasFullValuePrecision( predicates );
        }

        if ( predicate instanceof RangePredicate && predicate.valueGroup() == ValueGroup.NUMBER )
        {
            return instances[NUMBER].hasFullValuePrecision( predicates );
        }
        // TODO: support temporal range queries

        return false;
    }
}
