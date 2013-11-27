/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public class Neo4jJobSchedulerTest
{
    private Neo4jJobScheduler scheduler;

    @After
    public void stopScheduler()
    {
        scheduler.stop();
    }

    @Test
    public void shouldRunRecurringJob() throws Exception
    {
        // Given
        scheduler = new Neo4jJobScheduler( StringLogger.DEV_NULL );
        final AtomicInteger invocations = new AtomicInteger( 0 );

        // When
        scheduler.start();
        scheduler.scheduleRecurring( new Runnable()
        {
            public void run()
            {
                invocations.incrementAndGet();
            }
        }, 500, MILLISECONDS );
        sleep( 1500 );
        scheduler.stop();

        // Then
        int actualInvocations = invocations.get();
        assertTrue( actualInvocations >= 2); // <-- Dunno how to better assert that this works correctly :/
        assertTrue( actualInvocations < 6);

        sleep( 1000 );
        assertThat( invocations.get(), equalTo(actualInvocations) );
    }

}
