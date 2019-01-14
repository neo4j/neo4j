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

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

public class SpatialIndexUpdater extends SpatialIndexCache<NativeSchemaIndexUpdater<?, NativeSchemaValue>> implements IndexUpdater
{
    SpatialIndexUpdater( SpatialIndexAccessor accessor, IndexUpdateMode mode )
    {
        super( new PartFactory( accessor, mode ) );
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        IndexUpdater to = select( ((PointValue)update.values()[0]).getCoordinateReferenceSystem() );
        switch ( update.updateMode() )
        {
        case ADDED:
        case REMOVED:
            to.process( update );
            break;
        case CHANGED:
            IndexUpdater from = select( ((PointValue) update.beforeValues()[0]).getCoordinateReferenceSystem() );
            // There are two cases:
            // - both before/after go into the same updater --> pass update into that updater
            if ( from == to )
            {
                from.process( update );
            }
            // - before go into one and after into the other --> REMOVED from one and ADDED into the other
            else
            {
                from.process( IndexEntryUpdate.remove( update.getEntityId(), update.indexKey(), update.beforeValues() ) );
                to.process( IndexEntryUpdate.add( update.getEntityId(), update.indexKey(), update.values() ) );
            }
            break;
        default:
            throw new IllegalArgumentException( "Unknown update mode" );
        }
    }

    @Override
    public void close() throws IOException
    {
        forAll( NativeSchemaIndexUpdater::close, this );
    }

    static class PartFactory implements Factory<NativeSchemaIndexUpdater<?, NativeSchemaValue>>
    {

        private final SpatialIndexAccessor accessor;
        private final IndexUpdateMode mode;

        PartFactory( SpatialIndexAccessor accessor, IndexUpdateMode mode )
        {
            this.accessor = accessor;
            this.mode = mode;
        }

        @Override
        public NativeSchemaIndexUpdater<?,NativeSchemaValue> newSpatial( CoordinateReferenceSystem crs ) throws IOException
        {
            return accessor.select( crs ).newUpdater( mode );
        }
    }
}
