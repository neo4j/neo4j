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
package org.neo4j.causalclustering.core.consensus.schedule;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.logging.NullLog.getInstance;

/**
 * Most aspects of the Timer are tested through the {@link TimerServiceTest}.
 */
public class TimerTest
{
    @Rule
    public LifeRule lifeRule = new LifeRule( true );

    @Test
    public void shouldHandleConcurrentResetAndInvocationOfHandler() throws Exception
    {
        // given
        Neo4jJobScheduler scheduler = lifeRule.add( new Neo4jJobScheduler() );
        JobScheduler.Group group = new JobScheduler.Group( "test" );

        BinaryLatch invoked = new BinaryLatch();
        BinaryLatch done = new BinaryLatch();

        TimeoutHandler handler = timer ->
        {
            invoked.release();
            done.await();
        };

        Timer timer = new Timer( () -> "test", scheduler, getInstance(), group, handler );
        timer.set( new FixedTimeout( 0, SECONDS ) );
        invoked.await();

        // when
        timer.reset();

        // then: should not deadlock

        // cleanup
        done.release();
    }
}
