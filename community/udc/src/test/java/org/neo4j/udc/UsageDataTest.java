/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.udc;

import org.junit.jupiter.api.Test;

import org.neo4j.scheduler.JobScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.neo4j.udc.UsageDataKey.key;

class UsageDataTest
{
    @Test
    void shouldPutAndRetrieve()
    {
        // Given
        UsageData ms = new UsageData( mock( JobScheduler.class ) );
        UsageDataKey<String> key = key( "hello" );

        // When
        ms.set( key, "Hello!" );

        // Then
        assertEquals( "Hello!", ms.get( key ) );
        assertNull( ms.get( key( "other" ) ) );
    }
}
