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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;

class FusionIndexUpdater extends FusionIndexBase<IndexUpdater> implements IndexUpdater
{
    FusionIndexUpdater( SlotSelector slotSelector, LazyInstanceSelector<IndexUpdater> instanceSelector )
    {
        super( slotSelector, instanceSelector );
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        switch ( update.updateMode() )
        {
        case ADDED:
            instanceSelector.select( slotSelector.selectSlot( update.values(), GROUP_OF ) ).process( update );
            break;
        case CHANGED:
            // Hmm, here's a little conundrum. What if we change from a value that goes into native
            // to a value that goes into fallback, or vice versa? We also don't want to blindly pass
            // all CHANGED updates to both updaters since not all values will work in them.
            IndexUpdater from = instanceSelector.select( slotSelector.selectSlot( update.beforeValues(), GROUP_OF ) );
            IndexUpdater to = instanceSelector.select( slotSelector.selectSlot( update.values(), GROUP_OF ) );
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
            instanceSelector.select( slotSelector.selectSlot( update.values(), GROUP_OF ) ).process( update );
            break;
        default:
            throw new IllegalArgumentException( "Unknown update mode" );
        }
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        try
        {
            instanceSelector.close( IndexUpdater::close );
        }
        catch ( IOException | IndexEntryConflictException | RuntimeException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            // This catch-clause is basically only here to satisfy the compiler
            throw new RuntimeException( e );
        }
    }
}
