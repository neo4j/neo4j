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
import org.neo4j.values.storable.ValueGroup;

public class TemporalIndexPopulatingUpdater extends TemporalIndexCache<IndexUpdater> implements IndexUpdater
{
    TemporalIndexPopulatingUpdater( TemporalIndexPopulator populator, PropertyAccessor propertyAccessor )
    {
        super( new PartFactory( populator, propertyAccessor ) );
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        switch ( update.updateMode() )
        {
        case ADDED:
            select( update.values()[0].valueGroup() ).process( update );
            break;

        case CHANGED:
            // These are both temporal, but could belong in different parts
            IndexUpdater from = select( update.beforeValues()[0].valueGroup() );
            IndexUpdater to = select( update.values()[0].valueGroup() );
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
            select( update.values()[0].valueGroup() ).process( update );
            break;

        default:
            throw new IllegalArgumentException( "Unknown update mode" );
        }
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        for ( IndexUpdater part : this )
        {
            part.close();
        }
    }

    static class PartFactory implements TemporalIndexCache.Factory<IndexUpdater>
    {
        private final TemporalIndexPopulator populator;
        private PropertyAccessor propertyAccessor;

        PartFactory( TemporalIndexPopulator populator, PropertyAccessor propertyAccessor )
        {
            this.populator = populator;
            this.propertyAccessor = propertyAccessor;
        }

        @Override
        public IndexUpdater newDate() throws IOException
        {
            return populator.select(ValueGroup.DATE).newPopulatingUpdater( propertyAccessor );
        }

        @Override
        public IndexUpdater newLocalDateTime() throws IOException
        {
            return populator.select(ValueGroup.LOCAL_DATE_TIME).newPopulatingUpdater( propertyAccessor );
        }

        @Override
        public IndexUpdater newZonedDateTime() throws IOException
        {
            return populator.select(ValueGroup.ZONED_DATE_TIME).newPopulatingUpdater( propertyAccessor );
        }

        @Override
        public IndexUpdater newLocalTime() throws IOException
        {
            return populator.select(ValueGroup.LOCAL_TIME).newPopulatingUpdater( propertyAccessor );
        }

        @Override
        public IndexUpdater newZonedTime() throws IOException
        {
            return populator.select(ValueGroup.ZONED_TIME).newPopulatingUpdater( propertyAccessor );
        }

        @Override
        public IndexUpdater newDuration() throws IOException
        {
            return populator.select(ValueGroup.DURATION).newPopulatingUpdater( propertyAccessor );
        }
    }
}
