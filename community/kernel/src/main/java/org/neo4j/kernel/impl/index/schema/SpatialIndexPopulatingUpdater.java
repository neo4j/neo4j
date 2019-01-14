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
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;

public class SpatialIndexPopulatingUpdater extends SpatialIndexCache<IndexUpdater> implements IndexUpdater
{
    SpatialIndexPopulatingUpdater( SpatialIndexPopulator populator, PropertyAccessor propertyAccessor )
    {
        super( new PartFactory( populator, propertyAccessor ) );
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        PointValue value = (PointValue) update.values()[0];
        switch ( update.updateMode() )
        {
        case ADDED:
            select( value.getCoordinateReferenceSystem() ).process( update );
            break;

        case CHANGED:
            // These are both spatial, but could belong in different parts
            PointValue fromValue = (PointValue) update.beforeValues()[0];
            IndexUpdater from = select( fromValue.getCoordinateReferenceSystem() );
            IndexUpdater to = select( value.getCoordinateReferenceSystem() );
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

        case REMOVED:
            select( value.getCoordinateReferenceSystem() ).process( update );
            break;

        default:
            throw new IllegalArgumentException( "Unknown update mode" );
        }
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        for ( IndexUpdater updater : this )
        {
            updater.close();
        }
    }

    static class PartFactory implements Factory<IndexUpdater>
    {
        private final SpatialIndexPopulator populator;
        private PropertyAccessor propertyAccessor;

        PartFactory( SpatialIndexPopulator populator, PropertyAccessor propertyAccessor )
        {
            this.populator = populator;
            this.propertyAccessor = propertyAccessor;
        }

        @Override
        public IndexUpdater newSpatial( CoordinateReferenceSystem crs ) throws IOException
        {
            return populator.select( crs ).newPopulatingUpdater( propertyAccessor );
        }
    }
}
