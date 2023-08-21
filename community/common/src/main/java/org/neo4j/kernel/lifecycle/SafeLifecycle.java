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

import static org.neo4j.kernel.lifecycle.LifecycleStatus.NONE;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.SHUTDOWN;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.STARTED;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.STOPPED;

/**
 * A safer lifecycle adapter with strict semantics and as
 * a result simpler error handling and reasoning. If the
 * semantics of this class is not consistent with what you
 * are after, then it is probably better to create a new
 * adapter with the specific semantics required.
 * <pre>
 * Guide
 *
 *  *: No-op    (will not invoke operation)
 *  -: Error    (will throw IllegalStateException)
 *
 *  NONE:       ("pre initialization")
 *  STOPPED:    ("initialized" or "stopped")
 *  STARTED:    ("started")
 *  SHUTDOWN:   ("shutdown")
 * </pre>
 * A successful operation is an operation not throwing an exception.
 * <p>
 * End states on a successful operation.
 * <pre>
 * +----------+---------+---------+------------+-------------+
 * | FROM \op | init()  | start() |   stop()   | shutdown()  |
 * +----------+---------+---------+------------+-------------+
 * | NONE     | STOPPED | -       | -          | NONE(*)     |
 * | STOPPED  | -       | STARTED | STOPPED(*) | SHUTDOWN    |
 * | STARTED  | -       | -       | STOPPED    | -           |
 * | SHUTDOWN | -       | -       | -          | -           |
 * +----------+---------+---------+------------+-------------+
 * </pre>
 * End states on a failed operation.
 * <pre>
 *+----------+---------+---------+------------+-------------+
 * | FROM \op | init()  | start() |   stop()   | shutdown()  |
 * +----------+---------+---------+------------+-------------+
 * | NONE     | STOPPED | -       | -          | NONE(*)     |
 * | STOPPED  | -       | STOPPED | STOPPED(*) | SHUTDOWN    |
 * | STARTED  | -       | -       | STOPPED    | -           |
 * | SHUTDOWN | -       | -       | -          | -           |
 * +----------+---------+---------+------------+-------------+
 * </pre>
 * A few notes:
 * <ul>
 * <li>will not invoke stop0() if start0() wasn't successful</li>
 * <li>will not invoke shutdown0() if init0() wasn't successful</li>
 * </ul>
 * The expectation with regards to error handling and cleanup is that
 * an unclean start() is cleaned up by the start0() method and thus
 * the component is left in STOPPED. The same goes for issues happening
 * during init0().
 * <p>
 * Because of the way that {@link LifeSupport} operates today, this
 * class will ignore stop() calls made while in STOPPED. Similarly, calls
 * to shutdown() will be ignored while in NONE. This allows this class
 * to be managed by a {@link LifeSupport} without throwing
 * {@link IllegalStateException} on those state transitions, which
 * otherwise would have been disallowed and handled in the same way
 * as other illegal state transitions.
 * <p>
 * This adapter will not allow a shutdown lifecycle to be reinitialized
 * and started again.
 */
public abstract class SafeLifecycle implements Lifecycle, LifecycleStatusProvider {
    private volatile LifecycleStatus state;

    protected SafeLifecycle() {
        this(NONE);
    }

    SafeLifecycle(LifecycleStatus state) {
        this.state = state;
    }

    /**
     * @param expected The expected from state.
     * @param to The to state.
     * @param op The state transition operation.
     * @param force Causes the state to be updated regardless of if the operation throws.
     */
    private void transition(LifecycleStatus expected, LifecycleStatus to, Operation op, boolean force)
            throws Exception {
        if (state != expected) {
            throw new IllegalStateException(String.format("Expected %s but was %s", expected, state));
        }

        if (force) {
            state = to;
            op.run();
        } else {
            op.run();
            state = to;
        }
    }

    @Override
    public final synchronized void init() throws Exception {
        transition(NONE, STOPPED, this::init0, false);
    }

    @Override
    public final synchronized void start() throws Exception {
        transition(STOPPED, STARTED, this::start0, false);
    }

    @Override
    public final synchronized void stop() throws Exception {
        if (state == STOPPED) {
            return;
        }
        transition(STARTED, STOPPED, this::stop0, true);
    }

    @Override
    public final synchronized void shutdown() throws Exception {
        if (state == NONE) {
            state = NONE;
            return;
        }
        transition(STOPPED, SHUTDOWN, this::shutdown0, true);
    }

    public void init0() throws Exception {}

    public void start0() throws Exception {}

    public void stop0() throws Exception {}

    public void shutdown0() throws Exception {}

    @Override
    public LifecycleStatus getStatus() {
        return state;
    }

    interface Operation {
        void run() throws Exception;
    }
}
