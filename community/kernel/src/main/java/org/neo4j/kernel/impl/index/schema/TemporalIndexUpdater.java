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
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

public class TemporalIndexUpdater extends TemporalIndexCache<NativeSchemaIndexUpdater<?, NativeSchemaValue>> implements IndexUpdater
{
    TemporalIndexUpdater( TemporalIndexAccessor accessor, IndexUpdateMode mode )
    {
        super( new PartFactory( accessor, mode ) );
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        IndexUpdater to = select( update.values()[0].valueGroup() );
        switch ( update.updateMode() )
        {
        case ADDED:
        case REMOVED:
            to.process( update );
            break;
        case CHANGED:
            IndexUpdater from = select( update.beforeValues()[0].valueGroup() );
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

    static class PartFactory implements TemporalIndexCache.Factory<NativeSchemaIndexUpdater<?, NativeSchemaValue>>
    {

        private final TemporalIndexAccessor accessor;
        private final IndexUpdateMode mode;

        PartFactory( TemporalIndexAccessor accessor, IndexUpdateMode mode )
        {
            this.accessor = accessor;
            this.mode = mode;
        }

        @Override
        public NativeSchemaIndexUpdater<?, NativeSchemaValue> newDate() throws IOException
        {
            return accessor.select( ValueGroup.DATE ).newUpdater( mode );
        }

        @Override
        public NativeSchemaIndexUpdater<?, NativeSchemaValue> newLocalDateTime() throws IOException
        {
            return accessor.select(ValueGroup.LOCAL_DATE_TIME).newUpdater( mode );
        }

        @Override
        public NativeSchemaIndexUpdater<?, NativeSchemaValue> newZonedDateTime() throws IOException
        {
            return accessor.select(ValueGroup.ZONED_DATE_TIME).newUpdater( mode );
        }

        @Override
        public NativeSchemaIndexUpdater<?, NativeSchemaValue> newLocalTime() throws IOException
        {
            return accessor.select(ValueGroup.LOCAL_TIME).newUpdater( mode );
        }

        @Override
        public NativeSchemaIndexUpdater<?, NativeSchemaValue> newZonedTime() throws IOException
        {
            return accessor.select(ValueGroup.ZONED_TIME).newUpdater( mode );
        }

        @Override
        public NativeSchemaIndexUpdater<?, NativeSchemaValue> newDuration() throws IOException
        {
            return accessor.select(ValueGroup.DURATION).newUpdater( mode );
        }
    }
}
