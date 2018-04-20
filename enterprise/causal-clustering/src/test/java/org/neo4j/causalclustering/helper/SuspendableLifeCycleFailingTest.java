/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
