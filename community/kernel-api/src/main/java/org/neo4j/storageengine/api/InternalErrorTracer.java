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
package org.neo4j.storageengine.api;

import java.util.concurrent.atomic.LongAdder;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.kernel.api.exceptions.Status;

public interface InternalErrorTracer {
    long internalReadErrors();

    long internalWriteErrors();

    void traceReadError(Throwable throwable);

    void traceWriteError(Throwable throwable);

    InternalErrorTracer NO_TRACER = new InternalErrorTracer() {
        @Override
        public void traceReadError(Throwable throwable) {}

        @Override
        public void traceWriteError(Throwable throwable) {}

        @Override
        public long internalReadErrors() {
            return 0;
        }

        @Override
        public long internalWriteErrors() {
            return 0;
        }
    };

    class Impl implements InternalErrorTracer {
        private final LongAdder internalReadErrors = new LongAdder();
        private final LongAdder internalWriteErrors = new LongAdder();

        @Override
        public long internalReadErrors() {
            return internalReadErrors.sum();
        }

        @Override
        public long internalWriteErrors() {
            return internalWriteErrors.sum();
        }

        @Override
        public void traceReadError(Throwable throwable) {
            if (!isTransient(throwable)) {
                internalReadErrors.increment();
            }
        }

        @Override
        public void traceWriteError(Throwable throwable) {
            if (!isTransient(throwable)) {
                internalWriteErrors.increment();
            }
        }

        private static boolean isTransient(Throwable throwable) {
            return throwable instanceof TransientFailureException
                    || throwable instanceof Status.HasStatus hasStatus
                            && hasStatus.status().code().classification() == Status.Classification.TransientError;
        }
    }
}
