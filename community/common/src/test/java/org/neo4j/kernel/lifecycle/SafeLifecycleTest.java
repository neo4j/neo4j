/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.NONE;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.SHUTDOWN;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.STARTED;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.STOPPED;

import org.junit.jupiter.api.Test;
import org.neo4j.function.ThrowingConsumer;

class SafeLifecycleTest {
    private ThrowingConsumer<Lifecycle, Throwable> init = Lifecycle::init;
    private ThrowingConsumer<Lifecycle, Throwable> start = Lifecycle::start;
    private ThrowingConsumer<Lifecycle, Throwable> stop = Lifecycle::stop;
    private ThrowingConsumer<Lifecycle, Throwable> shutdown = Lifecycle::shutdown;

    @SuppressWarnings("unchecked")
    private ThrowingConsumer<Lifecycle, Throwable>[] ops = new ThrowingConsumer[] {init, start, stop, shutdown};

    private LifecycleStatus[] states = new LifecycleStatus[] {NONE, STOPPED, STARTED, SHUTDOWN};

    private Object[][] onSuccess = new Object[][] {
        //                       init()  start()  stop()  shutdown()
        new LifecycleStatus[] {STOPPED, null, null, NONE}, // from NONE
        new LifecycleStatus[] {null, STARTED, STOPPED, SHUTDOWN}, // from STOPPED
        new LifecycleStatus[] {null, null, STOPPED, null}, // from STARTED
        new LifecycleStatus[] {null, null, null, null}, // from SHUTDOWN
    };

    private Object[][] onFailed = new Object[][] {
        //                      init()  start()  stop()  shutdown()
        new LifecycleStatus[] {NONE, null, null, NONE}, // from NONE
        new LifecycleStatus[] {null, STOPPED, STOPPED, SHUTDOWN}, // from STOPPED
        new LifecycleStatus[] {null, null, STOPPED, null}, // from STARTED
        new LifecycleStatus[] {null, null, null, null}, // from SHUTDOWN
    };

    private Boolean[][] ignored = new Boolean[][] {
        //              init()  start()  stop()  shutdown()
        new Boolean[] {false, false, false, true}, // from NONE
        new Boolean[] {false, false, true, false}, // from STOPPED
        new Boolean[] {false, false, false, false}, // from STARTED
        new Boolean[] {false, false, false, false}, // from SHUTDOWN
    };

    @Test
    void shouldPerformSuccessfulTransitionsCorrectly() throws Throwable {
        for (int state = 0; state < states.length; state++) {
            for (int op = 0; op < ops.length; op++) {
                MySafeAndSuccessfulLife sf = new MySafeAndSuccessfulLife(states[state]);
                boolean caughtIllegalTransition = false;
                try {
                    ops[op].accept(sf);
                } catch (IllegalStateException e) {
                    caughtIllegalTransition = true;
                }

                if (onSuccess[state][op] == null) {
                    assertTrue(caughtIllegalTransition);
                    assertEquals(states[state], sf.getStatus());
                } else {
                    assertFalse(caughtIllegalTransition);
                    assertEquals(onSuccess[state][op], sf.getStatus());
                    int expectedOpCode = ignored[state][op] ? -1 : op;
                    assertEquals(expectedOpCode, sf.opCode);
                }
            }
        }
    }

    @Test
    void shouldPerformFailedTransitionsCorrectly() throws Throwable {
        for (int state = 0; state < states.length; state++) {
            for (int op = 0; op < ops.length; op++) {
                MyFailedLife sf = new MyFailedLife(states[state]);
                boolean caughtIllegalTransition = false;
                boolean failedOperation = false;
                try {
                    ops[op].accept(sf);
                } catch (IllegalStateException e) {
                    caughtIllegalTransition = true;
                } catch (UnsupportedOperationException e) {
                    failedOperation = true;
                }

                if (onFailed[state][op] == null) {
                    assertTrue(caughtIllegalTransition);
                    assertEquals(states[state], sf.getStatus());
                } else {
                    assertFalse(caughtIllegalTransition);
                    assertEquals(onFailed[state][op], sf.getStatus());

                    if (ignored[state][op]) {
                        assertEquals(-1, sf.opCode);
                        assertFalse(failedOperation);
                    } else {
                        assertEquals(op, sf.opCode);
                        assertTrue(failedOperation);
                    }
                }
            }
        }
    }

    private static class MySafeAndSuccessfulLife extends SafeLifecycle {
        int opCode;

        MySafeAndSuccessfulLife(LifecycleStatus state) {
            super(state);
            opCode = -1;
        }

        @Override
        public void init0() {
            invoke(0);
        }

        @Override
        public void start0() {
            invoke(1);
        }

        @Override
        public void stop0() {
            invoke(2);
        }

        @Override
        public void shutdown0() {
            invoke(3);
        }

        private void invoke(int opCode) {
            if (this.opCode == -1) {
                this.opCode = opCode;
            } else {
                throw new RuntimeException("Double invocation");
            }
        }
    }

    private static class MyFailedLife extends SafeLifecycle {
        int opCode;

        MyFailedLife(LifecycleStatus state) {
            super(state);
            opCode = -1;
        }

        @Override
        public void init0() {
            invoke(0);
        }

        @Override
        public void start0() {
            invoke(1);
        }

        @Override
        public void stop0() {
            invoke(2);
        }

        @Override
        public void shutdown0() {
            invoke(3);
        }

        private void invoke(int opCode) {
            if (this.opCode == -1) {
                this.opCode = opCode;
                throw new UnsupportedOperationException("I made a bo-bo");
            } else {
                throw new RuntimeException("Double invocation");
            }
        }
    }
}
