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
package org.neo4j.server.web;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.server.web.JettyThreadCalculator.MAX_THREADS;

class JettyThreadCalculatorTest
{
    @Test
    void shouldHaveCorrectAmountOfThreads()
    {
        JettyThreadCalculator jtc = new JettyThreadCalculator( 1 );
        assertEquals( 1, jtc.getAcceptors(), "Wrong acceptor value for 1 core" );
        assertEquals( 1, jtc.getSelectors(), "Wrong selector value for 1 core" );
        assertEquals( 12, jtc.getMaxThreads(), "Wrong maxThreads value for 1 core" );
        assertEquals( 6, jtc.getMinThreads(), "Wrong minThreads value for 1 core" );
        assertEquals( 480000, jtc.getMaxCapacity(), "Wrong capacity value for 1 core" );

        jtc = new JettyThreadCalculator( 4 );
        assertEquals( 1, jtc.getAcceptors(), "Wrong acceptor value for 4 cores" );
        assertEquals( 2, jtc.getSelectors(), "Wrong selector value for 4 cores" );
        assertEquals( 14, jtc.getMaxThreads(), "Wrong maxThreads value for 4 cores" );
        assertEquals( 8, jtc.getMinThreads(), "Wrong minThreads value for 4 cores" );
        assertEquals( 480000, jtc.getMaxCapacity(), "Wrong capacity value for 4 cores" );

        jtc = new JettyThreadCalculator( 16 );
        assertEquals( 2, jtc.getAcceptors(), "Wrong acceptor value for 16 cores" );
        assertEquals( 3, jtc.getSelectors(), "Wrong selector value for 16 cores" );
        assertEquals( 21, jtc.getMaxThreads(), "Wrong maxThreads value for 16 cores" );
        assertEquals( 14, jtc.getMinThreads(), "Wrong minThreads value for 16 cores" );
        assertEquals( 660000, jtc.getMaxCapacity(), "Wrong capacity value for 16 cores" );

        jtc = new JettyThreadCalculator( 64 );
        assertEquals( 4, jtc.getAcceptors(), "Wrong acceptor value for 64 cores" );
        assertEquals( 8, jtc.getSelectors(), "Wrong selector value for 64 cores" );
        assertEquals( 76, jtc.getMaxThreads(), "Wrong maxThreads value for 64 cores" );
        assertEquals( 36, jtc.getMinThreads(), "Wrong minThreads value for 64 cores" );
        assertEquals( 3120000, jtc.getMaxCapacity(), "Wrong capacity value for 64 cores" );

        jtc = new JettyThreadCalculator( MAX_THREADS );
        assertEquals( 2982, jtc.getAcceptors(), "Wrong acceptor value for max cores" );
        assertEquals( 5965, jtc.getSelectors(), "Wrong selector value for max cores" );
        assertEquals( 53685, jtc.getMaxThreads(), "Wrong maxThreads value for max cores" );
        assertEquals( 26841, jtc.getMinThreads(), "Wrong minThreads value for max cores" );
        assertEquals( 2147460000, jtc.getMaxCapacity(), "Wrong capacity value for max cores" );
    }

    @Test
    void shouldNotAllowLessThanOneThread()
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> new JettyThreadCalculator( 0 ) );
        assertEquals( "Max threads can't be less than 1", exception.getMessage() );
    }

    @Test
    void shouldNotAllowMoreThanMaxValue()
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> new JettyThreadCalculator( MAX_THREADS + 1 ) );
        assertEquals( String.format( "Max threads can't exceed %d", MAX_THREADS ), exception.getMessage() );
    }
}
