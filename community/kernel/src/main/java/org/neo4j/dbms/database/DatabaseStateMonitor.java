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
package org.neo4j.dbms.database;

import java.util.concurrent.atomic.AtomicLong;

public interface DatabaseStateMonitor {

    long getHosted();

    void increaseHosted();

    void decreaseHosted();

    long getFailed();

    void increaseFailed();

    void decreaseFailed();

    long getDesiredStarted();

    void setDesiredStarted(long newDesiredStarted);

    class Counter implements DatabaseStateMonitor {

        private final AtomicLong hosted = new AtomicLong(0);
        private final AtomicLong failed = new AtomicLong(0);
        private final AtomicLong desiredStarted = new AtomicLong(0);

        @Override
        public long getHosted() {
            return hosted.get();
        }

        @Override
        public void increaseHosted() {
            hosted.incrementAndGet();
        }

        @Override
        public void decreaseHosted() {
            hosted.decrementAndGet();
        }

        @Override
        public long getFailed() {
            return failed.get();
        }

        @Override
        public void increaseFailed() {
            failed.incrementAndGet();
        }

        @Override
        public void decreaseFailed() {
            failed.decrementAndGet();
        }

        @Override
        public long getDesiredStarted() {
            return desiredStarted.get();
        }

        @Override
        public void setDesiredStarted(long newDesiredStarted) {
            desiredStarted.set(newDesiredStarted);
        }
    }
}
