/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.kernel.impl.transaction.log.entry.DefaultLogEntryParserFactory;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserDispatcher;

import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class DefaultLogEntryParserFactoryTest
{

    private final DefaultLogEntryParserFactory factory = new DefaultLogEntryParserFactory();

    @Test
    public void shouldBeAbleToConstructAParserForLogVersion5()
    {
        assertNotNull( factory.newInstance( (byte) 5 ) );
    }

    @Test
    public void shouldBeAbleToConstructAParserForLogVersion4()
    {
        assertNotNull( factory.newInstance( (byte) 4 ) );
    }

    @Test(expected = IllegalStateException.class)
    public void shouldBeAbleToConstructAParserForAnyOtherVersion()
    {
        assertNotNull( factory.newInstance( (byte) 1 ) );
    }

    @Test
    public void shouldBeAbleToCacheAParserForLogVersion4()
    {
        LogEntryParserDispatcher first = factory.newInstance( (byte) 4 );
        assertNotNull( first );
        LogEntryParserDispatcher second = factory.newInstance( (byte) 4 );
        assertNotNull( second );
        assertSame( first, second );
    }

    @Test
    public void shouldBeAbleToCacheAParserForLogVersion5()
    {
        LogEntryParserDispatcher first = factory.newInstance( (byte) 5 );
        assertNotNull( first );
        LogEntryParserDispatcher second = factory.newInstance( (byte) 5 );
        assertNotNull( second );
        assertSame( first, second );
    }
}
