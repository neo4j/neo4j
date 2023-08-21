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
package org.neo4j.dbms.diagnostics.profile;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.neo4j.util.Preconditions;
import org.neo4j.util.concurrent.BinaryLatch;

abstract class ContinuousProfiler extends Profiler {
    private Thread worker;
    private final AtomicBoolean stopFlag = new AtomicBoolean();
    private final BinaryLatch startLatch = new BinaryLatch();

    @Override
    protected void start() {
        Preconditions.checkState(worker == null, "Already started");
        stopFlag.set(false);
        worker = new Thread(this::internalRun);
        worker.setName(getClass().getSimpleName() + " worker");
        worker.start();
        startLatch.await();
    }

    private void internalRun() {
        startLatch.release();
        try {
            run(stopFlag::get);
        } catch (RuntimeException exception) {
            setFailure(exception);
        }
    }

    protected abstract void run(BooleanSupplier stopCondition);

    @Override
    protected void stop() {
        Preconditions.checkState(worker != null, "Not started");
        stopFlag.set(true);
        try {
            worker.join(TimeUnit.MINUTES.toMillis(1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (worker.isAlive()) {
            throw new IllegalStateException(worker.getName() + " failed to stop");
        }
    }
}
