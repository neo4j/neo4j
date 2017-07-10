/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema.combined;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;

import static org.neo4j.kernel.impl.index.schema.combined.CombinedSchemaIndexProvider.select;

class CombinedIndexUpdater implements IndexUpdater
{
    private final IndexUpdater boostUpdater;
    private final IndexUpdater fallbackUpdater;

    CombinedIndexUpdater( IndexUpdater boostUpdater, IndexUpdater fallbackUpdater )
    {
        this.boostUpdater = boostUpdater;
        this.fallbackUpdater = fallbackUpdater;
    }

    @Override
    public void remove( PrimitiveLongSet nodeIds ) throws IOException
    {
        boostUpdater.remove( nodeIds );
        fallbackUpdater.remove( nodeIds );
    }

    @Override
    public void process( IndexEntryUpdate update ) throws IOException, IndexEntryConflictException
    {
        switch ( update.updateMode() )
        {
        case ADDED:
            select( update.values(), boostUpdater, fallbackUpdater ).process( update );
            break;
        case CHANGED:
            // Hmm, here's a little conundrum. What if we change from a value that goes into boost
            // to a value that goes into fallback, or vice versa? We also don't want to blindly pass
            // all CHANGED updates to both updaters since not all values will work in them.
            IndexUpdater from = select( update.beforeValues(), boostUpdater, fallbackUpdater );
            IndexUpdater to = select( update.values(), boostUpdater, fallbackUpdater );
            // There are two cases:
            // - both before/after go into the same updater --> pass update into that updater
            if ( from == to )
            {
                from.process( update );
            }
            // - before go into one and after into the other --> REMOVED from one and ADDED into the other
            else
            {
                from.process( IndexEntryUpdate.remove(
                        update.getEntityId(), update.indexKey(), update.beforeValues() ) );
                to.process( IndexEntryUpdate.add(
                        update.getEntityId(), update.indexKey(), update.values() ) );
            }
            break;
        case REMOVED:
            select( update.values(), boostUpdater, fallbackUpdater ).process( update );
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
            boostUpdater.close();
        }
        finally
        {
            fallbackUpdater.close();
        }
    }
}
