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
package org.neo4j.kernel.lifecycle;

import org.junit.Test;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.lifecycle.SafeLifecycle.State;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.lifecycle.SafeLifecycle.State.HALT;
import static org.neo4j.kernel.lifecycle.SafeLifecycle.State.IDLE;
import static org.neo4j.kernel.lifecycle.SafeLifecycle.State.PRE;
import static org.neo4j.kernel.lifecycle.SafeLifecycle.State.RUN;

public class SafeLifecycleTest
{
    private ThrowingConsumer<Lifecycle,Throwable> init = Lifecycle::init;
    private ThrowingConsumer<Lifecycle,Throwable> start = Lifecycle::start;
    private ThrowingConsumer<Lifecycle,Throwable> stop = Lifecycle::stop;
    private ThrowingConsumer<Lifecycle,Throwable> shutdown = Lifecycle::shutdown;
    @SuppressWarnings( "unchecked" )
    private ThrowingConsumer<Lifecycle,Throwable>[] ops = new ThrowingConsumer[]{init, start, stop, shutdown};

    private Object[][] onSuccess = new Object[][]{
            //            init()  start()  stop()  shutdown()
            new State[] { IDLE,    null,   null,     HALT }, // from PRE
            new State[] { null,    RUN,    IDLE,     HALT }, // from IDLE
            new State[] { null,    null,   IDLE,     null }, // from RUN
            new State[] { null,    null,   null,     null }, // from HALT
    };

    private Object[][] onFailed = new Object[][]{
            //            init()  start()  stop()  shutdown()
            new State[] { PRE,     null,   null,     HALT }, // from PRE
            new State[] { null,    IDLE,   IDLE,     HALT }, // from IDLE
            new State[] { null,    null,   IDLE,     null }, // from RUN
            new State[] { null,    null,   null,     null }, // from HALT
    };

    private Boolean[][] ignored = new Boolean[][]{
            //              init()  start()  stop()  shutdown()
            new Boolean[] { false,  false,   false,    true },  // from PRE
            new Boolean[] { false,  false,   true,     false }, // from IDLE
            new Boolean[] { false,  false,   false,    false }, // from RUN
            new Boolean[] { false,  false,   false,    false }, // from HALT
    };

    @Test
    public void shouldPerformSuccessfulTransitionsCorrectly() throws Throwable
    {
        for ( int state = 0; state < State.values().length; state++ )
        {
            for ( int op = 0; op < ops.length; op++ )
            {
                MySafeAndSuccessfulLife sf = new MySafeAndSuccessfulLife( State.values()[state] );
                boolean caughtIllegalTransition = false;
                try
                {
                    ops[op].accept( sf );
                }
                catch ( IllegalStateException e )
                {
                    caughtIllegalTransition = true;
                }

                if ( onSuccess[state][op] == null )
                {
                    assertTrue( caughtIllegalTransition );
                    assertEquals( State.values()[state], sf.state() );
                }
                else
                {
                    assertFalse( caughtIllegalTransition );
                    assertEquals( onSuccess[state][op], sf.state() );
                    int expectedOpCode = ignored[state][op] ? -1 : op;
                    assertEquals( expectedOpCode, sf.opCode );
                }
            }
        }
    }

    @Test
    public void shouldPerformFailedTransitionsCorrectly() throws Throwable
    {
        for ( int state = 0; state < State.values().length; state++ )
        {
            for ( int op = 0; op < ops.length; op++ )
            {
                MyFailedLife sf = new MyFailedLife( State.values()[state] );
                boolean caughtIllegalTransition = false;
                boolean failedOperation = false;
                try
                {
                    ops[op].accept( sf );
                }
                catch ( IllegalStateException e )
                {
                    caughtIllegalTransition = true;
                }
                catch ( UnsupportedOperationException e )
                {
                    failedOperation = true;
                }

                if ( onFailed[state][op] == null )
                {
                    assertTrue( caughtIllegalTransition );
                    assertEquals( State.values()[state], sf.state() );
                }
                else
                {
                    assertFalse( caughtIllegalTransition );
                    assertEquals( onFailed[state][op], sf.state() );

                    if ( ignored[state][op] )
                    {
                        assertEquals( -1, sf.opCode );
                        assertFalse( failedOperation );
                    }
                    else
                    {
                        assertEquals( op, sf.opCode );
                        assertTrue( failedOperation );
                    }
                }
            }
        }
    }

    private static class MySafeAndSuccessfulLife extends SafeLifecycle
    {
        int opCode;

        MySafeAndSuccessfulLife( State state )
        {
            super( state );
            opCode = -1;
        }

        @Override
        public void init0()
        {
            invoke( 0 );
        }

        @Override
        public void start0()
        {
            invoke( 1 );
        }

        @Override
        public void stop0()
        {
            invoke( 2 );
        }

        @Override
        public void shutdown0()
        {
            invoke( 3 );
        }

        private void invoke( int opCode )
        {
            if ( this.opCode == -1 )
            {
                this.opCode = opCode;
            }
            else
            {
                throw new RuntimeException( "Double invocation" );
            }
        }
    }

    private static class MyFailedLife extends SafeLifecycle
    {
        int opCode;

        MyFailedLife( State state )
        {
            super( state );
            opCode = -1;
        }

        @Override
        public void init0()
        {
            invoke( 0 );
        }

        @Override
        public void start0()
        {
            invoke( 1 );
        }

        @Override
        public void stop0()
        {
            invoke( 2 );
        }

        @Override
        public void shutdown0()
        {
            invoke( 3 );
        }

        private void invoke( int opCode )
        {
            if ( this.opCode == -1 )
            {
                this.opCode = opCode;
                throw new UnsupportedOperationException( "I made a bo-bo" );
            }
            else
            {
                throw new RuntimeException( "Double invocation" );
            }
        }
    }
}
