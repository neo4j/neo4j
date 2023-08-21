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
package org.neo4j.internal.counts;

import java.util.concurrent.locks.Lock;
import org.neo4j.counts.InvalidCountException;

/**
 * Manages a {@link CountWriter}, which is injectable, and a lock which it will release in {@link #close()}.
 */
public class CountUpdater implements AutoCloseable {
    private final CountWriter writer;
    private final Lock lock;

    CountUpdater(CountWriter writer, Lock lock) {
        this.writer = writer;
        this.lock = lock;
    }

    /**
     * Make a relative counts change to the given key.
     * @param key {@link CountsKey} the key to make the update for.
     * @param delta the delta for the count, can be positive or negative.
     * @return {@code true} if the absolute value either was 0 before this change, or became zero after the change. Otherwise {@code false}.
     */
    public boolean increment(CountsKey key, long delta) {
        try {
            return writer.write(key, delta);
        } catch (InvalidCountException e) {
            // This means that the value stored in the store associated with this key is invalid.
            // The fact was logged when the value that means 'invalid' was written to the store.
            // There is nothing better we can do here than just ignore updates to this key.
            // Throwing an exception here would mean panicking the database which is what this
            // entire invalid-value exercise tries to prevent
            return false;
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } finally {
            lock.unlock();
        }
    }

    interface CountWriter extends AutoCloseable {
        boolean write(CountsKey key, long delta);

        @Override
        void close();
    }
}
