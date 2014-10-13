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
package org.neo4j.kernel.impl.cache;

import org.junit.Test;

import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BridgingCacheAccessTest
{
    private final PersistenceCache persistenceCache = mock( PersistenceCache.class );
    private final SchemaState schemaState = mock( SchemaState.class );
    private final SchemaCache schemaCache = mock( SchemaCache.class );

    @Test
    public void shouldClearTheSchemaCacheWhenWeHitTheThreshold()
    {
        // GIVEN
        final BridgingCacheAccess cacheAccess = new BridgingCacheAccess( schemaCache, schemaState, persistenceCache );
        cacheAccess.setMaxChangesThreshold( 10 );

        // WHEN
        cacheAccess.applyCountUpdates( 1, 2, 3, 4 );

        // THEN
        verify( schemaState, times( 1 ) ).clear();
    }

    @Test
    public void shouldKeepACounterOfChangesUntilWeReachTheThreshold()
    {
        // GIVEN
        final BridgingCacheAccess cacheAccess = new BridgingCacheAccess( schemaCache, schemaState, persistenceCache );
        cacheAccess.setMaxChangesThreshold( 10 );

        // WHEN
        cacheAccess.applyCountUpdates( 1, 2, 3, 3 );
        verify( schemaState, never() ).clear();
        cacheAccess.applyCountUpdates( 0, 1, 0, 0 );

        // THEN
        verify( schemaState, times( 1 ) ).clear();
    }

    @Test
    public void shouldResetTheCountAfterAClear()
    {
        // GIVEN
        final BridgingCacheAccess cacheAccess = new BridgingCacheAccess( schemaCache, schemaState, persistenceCache );
        cacheAccess.setMaxChangesThreshold( 10 );
        cacheAccess.applyCountUpdates( 1, 2, 3, 4);
        verify( schemaState, times( 1 ) ).clear();
        reset( schemaCache );

        // WHEN
        cacheAccess.applyCountUpdates( 1, 0, 0, 0 );

        // THEN
        verify( schemaCache, never() ).clear();
    }
}
