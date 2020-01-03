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

import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

class SpatialVerifyDeferredConstraint
{
    static void verify( NodePropertyAccessor nodePropertyAccessor, IndexLayout<SpatialIndexKey,NativeIndexValue> layout,
            GBPTree<SpatialIndexKey,NativeIndexValue> tree, StoreIndexDescriptor descriptor ) throws IndexEntryConflictException
    {
        SpatialIndexKey from = layout.newKey();
        SpatialIndexKey to = layout.newKey();
        initializeKeys( from, to );
        try ( RawCursor<Hit<SpatialIndexKey,NativeIndexValue>,IOException> seek = tree.seek( from, to ) )
        {
            scanAndVerifyDuplicates( nodePropertyAccessor, descriptor, seek );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static void scanAndVerifyDuplicates( NodePropertyAccessor nodePropertyAccessor, StoreIndexDescriptor descriptor,
            RawCursor<Hit<SpatialIndexKey,NativeIndexValue>,IOException> seek ) throws IOException, IndexEntryConflictException
    {
        LongArrayList nodesWithCollidingPoints = new LongArrayList();
        long prevRawBits = Long.MIN_VALUE;

        // Bootstrap starting state
        if ( seek.next() )
        {
            Hit<SpatialIndexKey,NativeIndexValue> hit = seek.get();
            prevRawBits = hit.key().rawValueBits;
            nodesWithCollidingPoints.add( hit.key().getEntityId() );
        }

        while ( seek.next() )
        {
            Hit<SpatialIndexKey,NativeIndexValue> hit = seek.get();
            SpatialIndexKey key = hit.key();
            long currentRawBits = key.rawValueBits;
            long currentNodeId = key.getEntityId();
            if ( prevRawBits != currentRawBits )
            {
                if ( nodesWithCollidingPoints.size() > 1 )
                {
                    verifyConstraintOn( nodesWithCollidingPoints, nodePropertyAccessor, descriptor );
                }
                nodesWithCollidingPoints.clear();
            }
            nodesWithCollidingPoints.add( currentNodeId );
            prevRawBits = currentRawBits;
        }

        // Verify the last batch if needed
        if ( nodesWithCollidingPoints.size() > 1 )
        {
            verifyConstraintOn( nodesWithCollidingPoints, nodePropertyAccessor, descriptor );
        }
    }

    private static void verifyConstraintOn( LongArrayList nodeIds, NodePropertyAccessor nodePropertyAccessor, StoreIndexDescriptor descriptor )
            throws IndexEntryConflictException
    {
        MutableMap<Value,Long> points = Maps.mutable.empty();
        MutableLongIterator iter = nodeIds.longIterator();
        try
        {
            while ( iter.hasNext() )
            {
                long id = iter.next();
                Value value = nodePropertyAccessor.getNodePropertyValue( id, descriptor.schema().getPropertyId() );
                Long other = points.getIfAbsentPut( value, id );
                if ( other != id )
                {
                    throw new IndexEntryConflictException( other, id, value );
                }
            }
        }
        catch ( EntityNotFoundException e )
        {
            throw new RuntimeException( "Failed to validate uniqueness constraint", e );
        }
    }

    private static void initializeKeys( SpatialIndexKey from, SpatialIndexKey to )
    {
        from.initialize( Long.MIN_VALUE );
        to.initialize( Long.MAX_VALUE );
        from.initValueAsLowest( ValueGroup.GEOMETRY );
        to.initValueAsHighest( ValueGroup.GEOMETRY );
    }
}
