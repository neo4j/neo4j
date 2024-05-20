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
package org.neo4j.test;

import static java.lang.Long.max;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * Simple race scenario, a utility for executing multiple threads coordinated to start at the same time.
 * Add contestants with {@link #addContestant(Runnable)} and then when all have been added, start them
 * simultaneously using {@link #go()}, which will block until all contestants have completed.
 * Any errors from contestants are propagated out from {@link #go()}.
 */
public class Race {
    private static final int UNLIMITED = 0;

    public interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private final List<Contestant> contestants = new ArrayList<>();
    private volatile CountDownLatch readySet;
    private final CountDownLatch go = new CountDownLatch(1);
    private volatile int baseStartDelay;
    private volatile int maxRandomStartDelay;
    private volatile BooleanSupplier endCondition;
    private volatile boolean failure;
    private Consumer<Throwable> failureAction = f -> {};

    public Race withRandomStartDelays() {
        return withRandomStartDelays(10, 100);
    }

    public Race withRandomStartDelays(int base, int random) {
        this.baseStartDelay = base;
        this.maxRandomStartDelay = random;
        return this;
    }

    /**
     * Adds an end condition to this race. The race will end whenever an end condition is met
     * or when there's one contestant failing (throwing any sort of exception).
     *
     * @param endConditions one or more end conditions, such that when returning {@code true}
     * signals that the race should end.
     * @return this {@link Race} instance.
     */
    public Race withEndCondition(BooleanSupplier... endConditions) {
        for (BooleanSupplier endCondition : endConditions) {
            this.endCondition = mergeEndCondition(endCondition);
        }
        return this;
    }

    /**
     * Convenience for adding an end condition which is based on time. This will have contestants
     * end after the given duration (time + unit).
     *
     * @param time time value.
     * @param unit unit of time in {@link TimeUnit}.
     * @return this {@link Race} instance.
     */
    public Race withMaxDuration(long time, TimeUnit unit) {
        long endTimeNano = nanoTime() + unit.toNanos(time);
        this.endCondition = mergeEndCondition(() -> nanoTime() >= endTimeNano);
        return this;
    }

    public Race withFailureAction(Consumer<Throwable> failureAction) {
        this.failureAction = failureAction;
        return this;
    }

    private BooleanSupplier mergeEndCondition(BooleanSupplier additionalEndCondition) {
        BooleanSupplier existingEndCondition = endCondition;
        return existingEndCondition == null
                ? additionalEndCondition
                : () -> existingEndCondition.getAsBoolean() || additionalEndCondition.getAsBoolean();
    }

    /**
     * Convenience for wrapping contestants, especially for lambdas, which throws any sort of
     * checked exception.
     *
     * @param runnable actual contestant.
     * @return contestant wrapped in a try-catch (and re-throw as unchecked exception).
     */
    public static Runnable throwing(ThrowingRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
    }

    public void addContestants(int count, Runnable contestant) {
        addContestants(count, contestant, UNLIMITED);
    }

    public void addContestants(int count, Runnable contestant, int maxNumberOfRuns) {
        addContestants(count, i -> contestant, maxNumberOfRuns);
    }

    public void addContestants(int count, IntFunction<Runnable> contestantSupplier) {
        addContestants(count, contestantSupplier, UNLIMITED);
    }

    public void addContestants(int count, IntFunction<Runnable> contestantSupplier, int maxNumberOfRuns) {
        for (int i = 0; i < count; i++) {
            addContestant(contestantSupplier.apply(i), maxNumberOfRuns);
        }
    }

    public void addContestant(Runnable contestant) {
        addContestant(contestant, UNLIMITED);
    }

    public void addContestant(Runnable contestant, int maxNumberOfRuns) {
        contestants.add(new Contestant(contestant, contestants.size(), maxNumberOfRuns));
    }

    public void shuffleContestants() {
        Collections.shuffle(contestants);
    }

    /**
     * Starts the race and returns without waiting for contestants to complete.
     * @return Async instance for awaiting and get exceptions for the race.
     */
    public Async goAsync() {
        return startRace();
    }

    /**
     * Starts the race and waits indefinitely for all contestants to either fail or succeed.
     *
     * @throws Throwable on any exception thrown from any contestant.
     */
    public void go() throws Throwable {
        startRace().await(0, TimeUnit.MILLISECONDS);
    }

    /**
     * Starts the race and waits indefinitely for all contestants to either fail or succeed.
     *
     * @throws Throwable on any exception thrown from any contestant.
     */
    public void go(long maxWaitTime, TimeUnit unit) throws Throwable {
        startRace().await(maxWaitTime, unit);
    }

    /**
     * Like go, but wraps any exception in {@link RuntimeException}.
     */
    public void goUnchecked() {
        try {
            go();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Starts the race and waits {@code maxWaitTime} for all contestants to either fail or succeed.
     *
     * @return Async instance for awaiting and get exceptions for the race.
     */
    private Async startRace() {
        if (endCondition == null) {
            var unlimited = contestants.stream().anyMatch(c -> c.maxNumberOfRuns == UNLIMITED);
            endCondition = () -> unlimited;
        }

        readySet = new CountDownLatch(contestants.size());
        for (Contestant contestant : contestants) {
            contestant.start();
        }
        try {
            readySet.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Race couldn't start since race was interrupted while awaiting all contestants to start");
        }
        go.countDown();

        return (maxWaitTime, unit) -> {
            int errorCount = 0;
            long maxWaitTimeMillis = MILLISECONDS.convert(maxWaitTime, unit);
            long waitedSoFar = 0;
            for (Contestant contestant : contestants) {
                if (maxWaitTime == 0) {
                    contestant.join();
                } else {
                    long timeNanoStart = nanoTime();
                    contestant.join(max(1, maxWaitTimeMillis - waitedSoFar));
                    waitedSoFar += NANOSECONDS.toMillis(nanoTime() - timeNanoStart);
                    if (waitedSoFar >= maxWaitTimeMillis && contestant.isAlive()) {
                        throw new TimeoutException("Didn't complete after " + maxWaitTime + " " + unit);
                    }
                }
                if (contestant.error != null) {
                    errorCount++;
                }
            }

            if (errorCount > 1) {
                Throwable errors = new Throwable("Multiple errors found");
                for (Contestant contestant : contestants) {
                    if (contestant.error != null) {
                        errors.addSuppressed(contestant.error);
                    }
                }
                throw errors;
            }
            if (errorCount == 1) {
                for (Contestant contestant : contestants) {
                    if (contestant.error != null) {
                        throw contestant.error;
                    }
                }
            }
        };
    }

    public boolean hasFailed() {
        return failure;
    }

    private class Contestant extends Thread {
        private volatile Throwable error;
        private final int maxNumberOfRuns;
        private int runs;

        Contestant(Runnable code, int nr, int maxNumberOfRuns) {
            super(code, "Contestant#" + nr);
            this.maxNumberOfRuns = maxNumberOfRuns;
            this.setUncaughtExceptionHandler((thread, error) -> {});
        }

        @Override
        public void run() {
            readySet.countDown();
            try {
                go.await();
            } catch (InterruptedException e) {
                error = e;
                interrupt();
                return;
            }

            if (baseStartDelay > 0 || maxRandomStartDelay > 0) {
                randomlyDelaySlightly();
            }

            try {
                while (!failure) {
                    super.run();
                    if ((maxNumberOfRuns != UNLIMITED && ++runs == maxNumberOfRuns) || endCondition.getAsBoolean()) {
                        break;
                    }
                }
            } catch (Throwable e) {
                error = e;
                failure = true; // <-- global flag
                failureAction.accept(e);
                throw e;
            }
        }

        private void randomlyDelaySlightly() {
            int millis = ThreadLocalRandom.current().nextInt(maxRandomStartDelay);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(baseStartDelay + millis));
        }
    }

    public interface Async {
        void await(long waitTime, TimeUnit unit) throws Throwable;
    }
}
