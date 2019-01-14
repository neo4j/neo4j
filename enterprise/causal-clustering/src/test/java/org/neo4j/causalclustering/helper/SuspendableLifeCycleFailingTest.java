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
package org.neo4j.causalclustering.helper;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.function.ThrowingConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SuspendableLifeCycleFailingTest
{

    private CountingThrowingSuspendableLifeCycle lifeCycle;

    @Before
    public void setup() throws Throwable
    {
        lifeCycle = new CountingThrowingSuspendableLifeCycle();
        lifeCycle.init();
    }

    @Test
    public void canEnableIfStopFailed() throws Throwable
    {
        lifeCycle.start();
        lifeCycle.setFailMode();

        runFailing( SuspendableLifeCycle::stop );

        lifeCycle.setSuccessMode();

        lifeCycle.enable();

        assertEquals( 2, lifeCycle.starts );
    }

    @Test
    public void canEnableIfShutdownFailed() throws Throwable
    {
        lifeCycle.start();
        lifeCycle.setFailMode();

        runFailing( SuspendableLifeCycle::shutdown );

        lifeCycle.setSuccessMode();

        lifeCycle.enable();

        assertEquals( 2, lifeCycle.starts );
    }

    @Test
    public void canStartifDisableFailed() throws Throwable
    {
        lifeCycle.setFailMode();
        runFailing( SuspendableLifeCycle::disable );

        lifeCycle.setSuccessMode();
        lifeCycle.start();

        assertEquals( 1, lifeCycle.starts );
    }

    private void runFailing( ThrowingConsumer<SuspendableLifeCycle,Throwable> consumer ) throws Throwable
    {
        try
        {
            consumer.accept( lifeCycle );
            fail();
        }
        catch ( IllegalStateException ignore )
        {
        }
    }
}
