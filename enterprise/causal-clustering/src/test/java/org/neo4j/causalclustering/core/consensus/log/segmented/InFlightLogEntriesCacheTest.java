/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class InFlightLogEntriesCacheTest
{
    @Test
    public void shouldNotCacheUntilEnabled() throws Exception
    {
        InFlightMap<Object> cache = new InFlightMap<>();
        Object entry = new Object();

        cache.put( 1L, entry );
        assertNull( cache.get( 1L ) );

        cache.enable();
        cache.put( 1L, entry );
        assertEquals( entry, cache.get( 1L ) );
    }

    @Test
    public void shouldRegisterAndUnregisterValues() throws Exception
    {
        InFlightMap<Object> entries = new InFlightMap<>();
        entries.enable();

        Map<Long, Object> logEntryList = new HashMap<>();
        logEntryList.put(1L, new Object() );

        for ( Map.Entry<Long, Object> entry : logEntryList.entrySet() )
        {
            entries.put( entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<Long, Object> entry : logEntryList.entrySet() )
        {
            Object retrieved = entries.get( entry.getKey() );
            assertEquals( entry.getValue(), retrieved );
        }

        Long unexpected = 2L;
        Object shouldBeNull = entries.get( unexpected );
        assertNull( shouldBeNull );

        for ( Map.Entry<Long, Object> entry : logEntryList.entrySet() )
        {
            boolean wasThere = entries.remove( entry.getKey() );
            assertEquals( true, wasThere );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotReinsertValues() throws Exception
    {
        InFlightMap<Object> entries = new InFlightMap<>();
        entries.enable();
        Object addedObject = new Object();
        entries.put( 1L, addedObject );
        entries.put( 1L, addedObject );
    }

    @Test
    public void shouldNotReplaceRegisteredValues() throws Exception
    {
        InFlightMap<Object> cache = new InFlightMap<>();
        cache.enable();
        Object first = new Object();
        Object second = new Object();

        try
        {
            cache.put( 1L, first );
            cache.put( 1L, second );
            fail("Should not allow silent replacement of values");
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( first, cache.get( 1L ) );
        }
    }
}
