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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Test;

import org.neo4j.helpers.collection.Iterables;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TemporalIndexCacheTest
{
    @Test
    public void shouldIterateOverCreatedParts() throws Exception
    {
        TemporalIndexCache<String, Exception> cache = new TemporalIndexCache<>( new StringFactory() );

        assertEquals( Iterables.count( cache ), 0 );

        cache.localDateTime();
        cache.zonedTime();

        assertThat( cache, containsInAnyOrder( "LocalDateTime", "ZonedTime" ) );

        cache.date();
        cache.localTime();
        cache.localDateTime();
        cache.zonedTime();
        cache.zonedDateTime();
        cache.duration();

        assertThat( cache, containsInAnyOrder( "Date", "LocalDateTime", "ZonedDateTime", "LocalTime", "ZonedTime", "Duration" ) );
    }

    static class StringFactory implements TemporalIndexCache.Factory<String, Exception>
    {
        @Override
        public String newDate()
        {
            return "Date";
        }

        @Override
        public String newLocalDateTime()
        {
            return "LocalDateTime";
        }

        @Override
        public String newZonedDateTime()
        {
            return "ZonedDateTime";
        }

        @Override
        public String newLocalTime()
        {
            return "LocalTime";
        }

        @Override
        public String newZonedTime()
        {
            return "ZonedTime";
        }

        @Override
        public String newDuration()
        {
            return "Duration";
        }
    }
}
