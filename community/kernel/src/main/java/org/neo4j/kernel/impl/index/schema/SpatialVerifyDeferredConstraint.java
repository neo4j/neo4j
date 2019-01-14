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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongList;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.values.storable.Value;

class SpatialVerifyDeferredConstraint
{
    static void verify( PropertyAccessor nodePropertyAccessor, Layout<SpatialSchemaKey,NativeSchemaValue> layout,
            GBPTree<SpatialSchemaKey,NativeSchemaValue> tree, SchemaIndexDescriptor descriptor ) throws IndexEntryConflictException
    {
        SpatialSchemaKey from = layout.newKey();
        SpatialSchemaKey to = layout.newKey();
        initializeKeys( from, to );
        try ( RawCursor<Hit<SpatialSchemaKey,NativeSchemaValue>,IOException> seek = tree.seek( from, to ) )
        {
            scanAndVerifyDuplicates( nodePropertyAccessor, descriptor, seek );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static void scanAndVerifyDuplicates( PropertyAccessor nodePropertyAccessor, SchemaIndexDescriptor descriptor,
            RawCursor<Hit<SpatialSchemaKey,NativeSchemaValue>,IOException> seek ) throws IOException, IndexEntryConflictException
    {
        PrimitiveLongList nodesWithCollidingPoints = Primitive.longList();
        long prevRawBits = Long.MIN_VALUE;

        // Bootstrap starting state
        if ( seek.next() )
        {
            Hit<SpatialSchemaKey,NativeSchemaValue> hit = seek.get();
            prevRawBits = hit.key().rawValueBits;
            nodesWithCollidingPoints.add( hit.key().getEntityId() );
        }

        while ( seek.next() )
        {
            Hit<SpatialSchemaKey,NativeSchemaValue> hit = seek.get();
            SpatialSchemaKey key = hit.key();
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

    private static void verifyConstraintOn( PrimitiveLongList nodeIds, PropertyAccessor nodePropertyAccessor, SchemaIndexDescriptor descriptor )
            throws IndexEntryConflictException
    {
        Map<Value,Long> points = new HashMap<>();
        PrimitiveLongIterator iter = nodeIds.iterator();
        try
        {
            while ( iter.hasNext() )
            {
                long id = iter.next();
                Value value = nodePropertyAccessor.getPropertyValue( id, descriptor.schema().getPropertyId() );
                Long other = points.get( value );
                if ( other == null )
                {
                    points.put( value, id );
                    other = id;
                }
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

    private static void initializeKeys( SpatialSchemaKey from, SpatialSchemaKey to )
    {
        from.initialize( Long.MIN_VALUE );
        to.initialize( Long.MAX_VALUE );
        from.initValueAsLowest();
        to.initValueAsHighest();
    }
}
