/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core.consensus.log.segmented;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class InFlightLogEntriesCacheTest
{
    @Test
    public void shouldRegisterAndUnregisterValues() throws Exception
    {
        InFlightMap<Object,Object> entries = new InFlightMap<>();

        List<Object> logEntryList = new LinkedList<>();
        logEntryList.add( new Object() );

        for ( Object registeredEntry : logEntryList )
        {
            entries.register( registeredEntry, registeredEntry );
        }

        for ( Object expected : logEntryList )
        {
            Object retrieved = entries.retrieve( expected );
            assertEquals( expected, retrieved );
        }

        Object unexpected = new Object();
        Object shouldBeNull = entries.retrieve( unexpected );
        assertNull( shouldBeNull );

        for ( Object expected : logEntryList )
        {
            boolean wasThere = entries.unregister( expected );
            assertEquals( true, wasThere );
        }

        boolean shouldBeFalse = entries.unregister( unexpected );
        assertFalse( shouldBeFalse );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotReinsertValues() throws Exception
    {
        InFlightMap<Object,Object> entries = new InFlightMap<>();
        Object addedObject = new Object();
        entries.register( addedObject, addedObject );
        entries.register( addedObject, addedObject );
    }

    @Test
    public void shouldNotReplaceRegisteredValues() throws Exception
    {
        InFlightMap<Object,Object> cache = new InFlightMap<>();
        Object first = new Object();
        Object second = new Object();

        try
        {
            cache.register( first, first );
            cache.register( first, second );
            fail("Should not allow silent replacement of values");
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( first, cache.retrieve( first ) );
        }
    }
}
