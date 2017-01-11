/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.DuplicateIndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.impl.util.DiffSets;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public abstract class UniquePropertyIndexUpdater implements IndexUpdater
{
    public interface Lookup
    {
        Long currentlyIndexedNode( Object value ) throws IOException;
    }

    private final Map<Object, DiffSets<Long>> referenceCount = new HashMap<>();
    private final ArrayList<NodePropertyUpdate> updates = new ArrayList<>();

    private final Lookup lookup;

    public UniquePropertyIndexUpdater( Lookup lookup )
    {
        this.lookup = lookup;
    }

    @Override
    public void process( NodePropertyUpdate update )
    {
        // build uniqueness verification state
        switch ( update.getUpdateMode() )
        {
            case ADDED:
                propertyValueDiffSet( update.getValueAfter() ).add( update.getNodeId() );
                break;
            case CHANGED:
                propertyValueDiffSet( update.getValueBefore() ).remove( update.getNodeId() );
                propertyValueDiffSet( update.getValueAfter() ).add( update.getNodeId() );
                break;
            case REMOVED:
                propertyValueDiffSet( update.getValueBefore() ).remove( update.getNodeId() );
                break;
            default:
                throw new UnsupportedOperationException();
        }

        // do not flush update before close
        updates.add( update );
    }


    @Override
    public void close() throws IOException, IndexEntryConflictException, IndexCapacityExceededException
    {
        // verify uniqueness
        for ( Map.Entry<Object, DiffSets<Long>> entry : referenceCount.entrySet() )
        {
            Object value = entry.getKey();
            int delta = entry.getValue().delta();
            if ( delta > 1 )
            {
                throw new DuplicateIndexEntryConflictException( value, asSet( entry.getValue().getAdded() ) );
            }
            if ( delta == 1 )
            {
                Long addedNode = single( entry.getValue().getAdded() );
                Long existingNode = lookup.currentlyIndexedNode( value );

                if ( existingNode != null && !addedNode.equals( existingNode ) )
                {
                    throw new PreexistingIndexEntryConflictException( value, existingNode, addedNode );
                }
            }
        }
        // flush updates
        flushUpdates( updates );
    }

    protected abstract void flushUpdates( Iterable<NodePropertyUpdate> updates )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException;

    private DiffSets<Long> propertyValueDiffSet( Object value )
    {
        DiffSets<Long> diffSets = referenceCount.get( value );
        if ( diffSets == null )
        {
            referenceCount.put( value, diffSets = new DiffSets<>() );
        }
        return diffSets;
    }
}
