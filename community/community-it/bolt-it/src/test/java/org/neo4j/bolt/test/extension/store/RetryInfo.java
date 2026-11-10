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
package org.neo4j.bolt.test.extension.store;

import java.util.function.Supplier;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;

/**
 * Encapsulates the number of performed retries as well as the maximum permitted number of retries
 * for a given test case.
 *
 * @param count the number of attempted retries (including the current retry if any).
 * @param max   the maximum number of permitted retries.
 */
public record RetryInfo(int count, int max) {

    private static final Namespace NAMESPACE = Namespace.create(RetryInfo.class);

    /**
     * Retrieves the retry information published for a given test case context or publishes a new
     * empty retry information object to the context.
     *
     * @param context  a test context.
     * @param supplier a factory capable of creating empty retry information objects.
     * @return a retry information object.
     */
    public static RetryInfo getOrCreate(ExtensionContext context, Supplier<RetryInfo> supplier) {
        var method = context.getRequiredTestMethod();
        var store = context.getStore(NAMESPACE);

        return (RetryInfo) store.getOrComputeIfAbsent(method, key -> supplier.get());
    }

    /**
     * Publishes this retry information object to the given test context.
     *
     * @param context a target test context.
     */
    public void publish(ExtensionContext context) {
        var method = context.getRequiredTestMethod();
        var store = context.getStore(NAMESPACE);

        store.put(method, this);
    }

    /**
     * Evaluates whether another retry is permissible based on the current state of this retry
     * information object.
     *
     * @return true if at least one additional retry is permissible, false otherwise.
     */
    public boolean mayRetry() {
        if (this.max == 0) {
            return true;
        }

        return this.count + 1 <= this.max;
    }

    /**
     * Increments the number of retries performed and returns the resulting retry information object.
     * <p/>
     * Please note that retry information objects are immutable. The returned instance will have to be
     * subsequently published to the test context in order to be discovered within future
     * invocations.
     *
     * @return a mutated retry info object.
     */
    public RetryInfo increment() {
        return new RetryInfo(this.count + 1, this.max);
    }

    @Override
    public String toString() {
        return this.count + " / " + this.max;
    }
}
