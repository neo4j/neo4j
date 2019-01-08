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
package org.neo4j.kernel.impl.transaction;

import org.junit.Test;

import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;

import static org.junit.Assert.assertEquals;

public class LogHeaderCacheTest
{
    @Test
    public void shouldReturnNullWhenThereIsNoHeaderInTheCache()
    {
        // given
        final LogHeaderCache cache = new LogHeaderCache( 2 );

        // when
        final Long logHeader = cache.getLogHeader( 5 );

        // then
        assertEquals( null, logHeader );
    }

    @Test
    public void shouldReturnTheHeaderIfInTheCache()
    {
        // given
        final LogHeaderCache cache = new LogHeaderCache( 2 );

        // when
        cache.putHeader( 5, 3 );
        final long logHeader = cache.getLogHeader( 5 );

        // then
        assertEquals( 3, logHeader );
    }

    @Test
    public void shouldClearTheCache()
    {
        // given
        final LogHeaderCache cache = new LogHeaderCache( 2 );

        // when
        cache.putHeader( 5, 3 );
        cache.clear();
        final Long logHeader = cache.getLogHeader( 5 );

        // then
        assertEquals( null, logHeader );
    }
}
