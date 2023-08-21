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

import org.neo4j.internal.helpers.Exceptions;

abstract class Profiler {
    private volatile RuntimeException failure;

    void startProfiling() {
        failureSafeOp(this::start);
    }

    void stopProfiling() {
        failureSafeOp(this::stop);
    }

    RuntimeException failure() {
        return failure;
    }

    protected abstract boolean available();

    protected synchronized void setFailure(RuntimeException exception) {
        failure = Exceptions.chain(failure, exception);
    }

    private void failureSafeOp(Runnable op) {
        try {
            op.run();
        } catch (RuntimeException exception) {
            setFailure(exception);
        }
    }

    protected abstract void start();

    protected abstract void stop();
}
