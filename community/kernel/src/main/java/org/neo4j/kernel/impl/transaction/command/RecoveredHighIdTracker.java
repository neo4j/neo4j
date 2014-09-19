/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.CommonAbstractStore;

public class RecoveredHighIdTracker implements HighIdTracker
{
    private final Map<CommonAbstractStore, HighId> highIds = new HashMap<>();
    
    @Override
    public void track( CommonAbstractStore store, long id )
    {
        HighId highId = highIds.get( store );
        if ( highId == null )
        {
            highIds.put( store, highId = new HighId( id ) );
        }
        else
        {
            highId.track( id );
        }
    }
    
    private static class HighId
    {
        private long id;

        public HighId( long id )
        {
            this.id = id;
        }
        
        void track( long id )
        {
            if ( id > this.id )
            {
                this.id = id;
            }
        }
    }

    @Override
    public void apply()
    {
        // Notifies the stores about the recovered ids and will bump those high ids atomically if
        // they surpass the current high ids
        for ( Map.Entry<CommonAbstractStore, HighId> highId : highIds.entrySet() )
        {
            highId.getKey().setHighestPossibleIdInUse( highId.getValue().id );
        }
    }
}
