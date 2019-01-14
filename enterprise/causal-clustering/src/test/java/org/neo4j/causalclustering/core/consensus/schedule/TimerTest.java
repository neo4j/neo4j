/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.schedule;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
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
    public void shouldHandleConcurrentResetAndInvocationOfHandler()
    {
        // given
        CentralJobScheduler scheduler = lifeRule.add( new CentralJobScheduler() );
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
