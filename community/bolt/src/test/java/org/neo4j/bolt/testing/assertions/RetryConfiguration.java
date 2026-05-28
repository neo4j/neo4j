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
package org.neo4j.bolt.testing.assertions;

import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

public final class RetryConfiguration {
    private final ConditionFactory awaitility;
    private final Runnable preparationFunc;
    private final Runnable finalizerFunc;

    private RetryConfiguration(ConditionFactory awaitility, Runnable preparationFunc, Runnable finalizerFunc) {
        this.awaitility = awaitility;
        this.preparationFunc = preparationFunc;
        this.finalizerFunc = finalizerFunc;
    }

    private RetryConfiguration() {
        this(
                Awaitility.await()
                        .atMost(2, TimeUnit.MINUTES)
                        .pollInterval(100, TimeUnit.MILLISECONDS)
                        .pollDelay(10, TimeUnit.SECONDS)
                        .pollInSameThread(),
                null,
                null);
    }

    public static RetryConfiguration create() {
        return new RetryConfiguration();
    }

    public void evaluate(Runnable assertions) {
        this.awaitility.untilAsserted(() -> {
            if (this.preparationFunc != null) {
                this.preparationFunc.run();
            }

            try {
                assertions.run();
            } finally {
                if (this.finalizerFunc != null) {
                    this.finalizerFunc.run();
                }
            }
        });
    }

    private RetryConfiguration mutate(ConditionFactory awaitility) {
        return new RetryConfiguration(awaitility, this.preparationFunc, this.finalizerFunc);
    }

    public RetryConfiguration waitAtMost(long time, TimeUnit unit) {
        return this.mutate(this.awaitility.atMost(time, unit));
    }

    public RetryConfiguration pollInterval(long interval, TimeUnit unit) {
        return this.mutate(this.awaitility.pollInterval(interval, unit));
    }

    public RetryConfiguration pollDelay(long delay, TimeUnit unit) {
        return this.mutate(this.awaitility.pollDelay(delay, unit));
    }

    public RetryConfiguration onPrepare(Runnable preparationFunc) {
        return new RetryConfiguration(this.awaitility, preparationFunc, this.finalizerFunc);
    }

    public RetryConfiguration onFinalize(Runnable finalizerFunc) {
        return new RetryConfiguration(this.awaitility, this.preparationFunc, finalizerFunc);
    }
}
