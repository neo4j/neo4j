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
package org.neo4j.kernel.impl.api.index.updater;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;

/**
 * This IndexUpdater ensures that updated properties abide by uniqueness constraints. Updates are grouped up in
 * {@link #process(IndexEntryUpdate)}, and verified in {@link #close()}.
 *
 */
public abstract class UniquePropertyIndexUpdater implements IndexUpdater
{
    private final Map<Object,PrimitiveLongDiffSets> referenceCount = new HashMap<>();
    private final ArrayList<IndexEntryUpdate<?>> updates = new ArrayList<>();

    @Override
    public void process( IndexEntryUpdate<?> update )
    {
        // build uniqueness verification state
        switch ( update.updateMode() )
        {
            case ADDED:
                propertyValueDiffSet( update.values() ).add( update.getEntityId() );
                break;
            case CHANGED:
                propertyValueDiffSet( update.beforeValues() ).remove( update.getEntityId() );
                propertyValueDiffSet( update.values() ).add( update.getEntityId() );
                break;
            case REMOVED:
                propertyValueDiffSet( update.values() ).remove( update.getEntityId() );
                break;
            default:
                throw new UnsupportedOperationException();
        }

        // do not flush update before close
        updates.add( update );
    }

    @Override
    public void close()
    {
        // flush updates
        flushUpdates( updates );
    }

    protected abstract void flushUpdates( Iterable<IndexEntryUpdate<?>> updates );

    private PrimitiveLongDiffSets propertyValueDiffSet( Object value )
    {
        return referenceCount.computeIfAbsent( value, k -> new PrimitiveLongDiffSets() );
    }
}
