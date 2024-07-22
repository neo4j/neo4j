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
package org.neo4j.adversaries;

/**
 * An adversary that injects failures randomly, based on a configured probability.
 */
@SuppressWarnings("unchecked")
public final class RandomAdversary extends AbstractAdversary {
    private final double mischiefRate;
    private final double failureRate;
    private final double errorRate;
    private volatile boolean enabled;

    public RandomAdversary(double mischiefRate, double failureRate, double errorRate) {
        assert 0 <= mischiefRate && mischiefRate < 1.0 : "Expected mischief rate in [0.0; 1.0[ but was " + mischiefRate;
        assert 0 <= failureRate && failureRate < 1.0 : "Expected failure rate in [0.0; 1.0[ but was " + failureRate;
        assert 0 <= errorRate && errorRate < 1.0 : "Expected error rate in [0.0; 1.0[ but was " + errorRate;
        assert mischiefRate + errorRate + failureRate < 1.0
                : "Expected mischief rate + error rate + failure rate in [0.0; 1.0[ but was "
                        + (mischiefRate + errorRate + failureRate);

        this.mischiefRate = mischiefRate;
        this.failureRate = failureRate;
        this.errorRate = errorRate;
        enabled = true;
    }

    @Override
    public void injectFailure(Class<? extends Throwable>... failureTypes) {
        maybeDoBadStuff(failureTypes, false);
    }

    @Override
    public boolean injectFailureOrMischief(Class<? extends Throwable>... failureTypes) {
        return maybeDoBadStuff(failureTypes, true);
    }

    private boolean maybeDoBadStuff(Class<? extends Throwable>[] failureTypes, boolean includingMischeif) {
        if (!enabled) {
            return false;
        }

        double luckyDraw = rng.nextDouble();
        if (luckyDraw <= errorRate) {
            throwOneOf(OutOfMemoryError.class, NullPointerException.class);
        } else if (failureTypes.length > 0 && luckyDraw <= (failureRate + errorRate)) {
            throwOneOf(failureTypes);
        }
        return includingMischeif && luckyDraw <= (mischiefRate + failureRate + errorRate);
    }

    public void enableAdversary(boolean enabled) {
        this.enabled = enabled;
    }
}
