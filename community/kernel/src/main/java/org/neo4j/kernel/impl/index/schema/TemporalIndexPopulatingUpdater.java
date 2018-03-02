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

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils;

public class TemporalIndexPopulatingUpdater extends TemporalIndexCache<NativePopulatingUpdater,IOException> implements IndexUpdater
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
            NativePopulatingUpdater from = select( update.beforeValues()[0].valueGroup() );
            NativePopulatingUpdater to = select( update.values()[0].valueGroup() );
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
        FusionIndexUtils.forAll( NativePopulatingUpdater::close, this );
    }

    static class PartFactory implements TemporalIndexCache.Factory<NativePopulatingUpdater,IOException>
    {
        private final TemporalIndexPopulator populator;
        private PropertyAccessor propertyAccessor;

        PartFactory( TemporalIndexPopulator populator, PropertyAccessor propertyAccessor )
        {
            this.populator = populator;
            this.propertyAccessor = propertyAccessor;
        }

        @Override
        public NativePopulatingUpdater newDate() throws IOException
        {
            return populator.date().newPopulatingUpdater( propertyAccessor );
        }

        @Override
        public NativePopulatingUpdater newDateTime() throws IOException
        {
            throw new UnsupportedOperationException( "too tired to write" );
        }

        @Override
        public NativePopulatingUpdater newDateTimeZoned() throws IOException
        {
            throw new UnsupportedOperationException( "too tired to write" );
        }

        @Override
        public NativePopulatingUpdater newTime() throws IOException
        {
            throw new UnsupportedOperationException( "too tired to write" );
        }

        @Override
        public NativePopulatingUpdater newTimeZoned() throws IOException
        {
            throw new UnsupportedOperationException( "too tired to write" );
        }

        @Override
        public NativePopulatingUpdater newDuration() throws IOException
        {
            throw new UnsupportedOperationException( "too tired to write" );
        }
    }
}
